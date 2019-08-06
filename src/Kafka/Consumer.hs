{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}

module Kafka.Consumer
  ( consumerSession
  , merge
  ) where

import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.Except
import Data.ByteString (ByteString)
import Data.Foldable
import Data.Int
import Data.IntMap (IntMap)
import Data.Maybe
import GHC.Conc

import qualified Data.ByteString.Char8 as B
import qualified Data.IntMap as IM

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.Heartbeat.Response
import Kafka.JoinGroup.Response (Member, getJoinGroupResponse)
import Kafka.LeaveGroup.Response
import Kafka.ListOffsets.Response (ListOffsetsResponse)
import Kafka.OffsetFetch.Response (OffsetFetchResponse)
import Kafka.SyncGroup.Response (SyncTopicAssignment)

import qualified Kafka.Fetch.Response as F
import qualified Kafka.JoinGroup.Response as J
import qualified Kafka.ListOffsets.Response as L
import qualified Kafka.OffsetCommit.Response as C
import qualified Kafka.OffsetFetch.Response as O
import qualified Kafka.SyncGroup.Response as S

newtype Consumer a
  = Consumer { runConsumer :: ExceptT KafkaException IO a }
  deriving (Functor, Applicative, Monad)

tryParse :: Either KafkaException (Either String a) -> Either KafkaException a
tryParse = \case
  Right (Right parsed) -> Right parsed
  Right (Left parseError) -> Left (KafkaParseException parseError)
  Left networkError -> Left networkError

instance MonadIO Consumer where
  liftIO = Consumer . ExceptT . fmap Right

liftConsumer :: IO (Either KafkaException a) -> Consumer a
liftConsumer = Consumer . ExceptT

throwConsumer :: KafkaException -> Consumer a
throwConsumer = Consumer . throwError

-- | This module provides a high-level interface to the Kafka API for
-- consumers by wrapping the low-level request and response type modules.

-- | Get the earliest offsets corresponding to unread messages on the topic.
-- The group may not have committed an offset on a given partition yet, so we
-- need to send a ListOffsets request to find out which offsets are even valid
-- on the topic and default to that offset if a committed offset is not
-- present.
getInitialOffsets ::
     Kafka
  -> GroupMember
  -> Topic
  -> [Int32]
  -> IO (Maybe (IntMap Int64))
getInitialOffsets kafka member topic indices = do
  let Topic name _ _ = topic
  let topicName = TopicName name
  let name' = toByteString name
  void $ listOffsets kafka topicName indices
  listedOffs <- L.getListOffsetsResponse kafka =<< registerDelay defaultTimeout
  void $ offsetFetch kafka member topicName indices
  committedOffs <- O.getOffsetFetchResponse kafka =<< registerDelay defaultTimeout
  case (listedOffs, committedOffs) of
    (Right (Right lor), Right (Right ofr)) ->
      case (listOffsetsMap name' lor, fetchOffsetsMap name' ofr) of
        (Just lm, Just om) -> do
          let res = merge lm om
          pure (Just res)
        _ -> pure Nothing
    _ -> pure Nothing

toOffsetList :: IntMap Int64 -> [PartitionOffset]
toOffsetList = map (\(k, v) -> PartitionOffset (fromIntegral k) v) . IM.toList

listOffsetsMap :: ByteString -> ListOffsetsResponse -> Maybe (IntMap Int64)
listOffsetsMap topicName lor = do
  thisTopic <- find (\t -> L.topic t == topicName) (L.responses lor)
  pure . IM.fromList $
    map
      (\pr -> (fromIntegral (L.partition pr), L.offset pr))
      (L.partitionResponses thisTopic)

fetchOffsetsMap :: ByteString -> OffsetFetchResponse -> Maybe (IntMap Int64)
fetchOffsetsMap topicName ofr = do
  thisTopic <- find (\t -> O.offsetFetchTopic t == topicName) (O.topics ofr)
  pure . IM.fromList $
    map
      (\pr ->
        (fromIntegral (O.offsetFetchPartitionIndex pr)
        , O.offsetFetchOffset pr))
      (O.offsetFetchPartitions thisTopic)

merge :: IntMap Int64 -> IntMap Int64 -> IntMap Int64
merge lor ofr = mergeId (\l o -> Just $ if o < 0 then l else o) lor ofr
  where
  mergeId f a b = IM.mergeWithKey (\_ left right -> f left right) id id a b

defaultTimeout :: Int
defaultTimeout = 30000000

fiveSeconds :: Int
fiveSeconds = 5000000

updateOffsets :: TopicName -> IntMap Int64 -> FetchResponse -> IntMap Int64
updateOffsets (TopicName topicName) current r =
  let
    topicResps = filter
      (\x -> F.fetchResponseTopic x == toByteString topicName)
      (F.responses r)
    partitionResps = concatMap partitionResponses topicResps
    getPartitionResponse pid =
      find
        (\resp -> pid == partition (partitionHeader resp))
        partitionResps
  in
    IM.mapWithKey
      (\pid offs ->
        maybe
          offs
          (highWatermark . partitionHeader)
          (getPartitionResponse (fromIntegral pid)))
      current

data ConsumerState = ConsumerState
  { currentMember :: GroupMember
  , currentGenId :: GenerationId
  , currentHeartbeat :: Maybe (Either KafkaException Int16)
  } deriving (Show)

consumerSession ::
     Kafka
  -> Topic
  -> GroupMember
  -> String
  -> IO (Either KafkaException ())
consumerSession kafka top oldMe name = runExceptT $ runConsumer $ do
  let topicName = getTopicName top
  (genId, me, members) <- joinG kafka topicName oldMe
  (newGenId, assigns) <- sync kafka top me members genId name
  case partitionsForTopic topicName assigns of
    Just indices -> do
      offs <- liftIO $ getInitialOffsets kafka me top indices
      case offs of
        Just initialOffsets -> do
          currentState <- liftIO $ newTVarIO
            (ConsumerState me newGenId Nothing)
          void $ liftIO $ forkIO $ withDefaultKafka $ \k ->
            heartbeats k currentState name
          void $
            loop kafka top currentState initialOffsets indices name
        Nothing -> fail "The topic was not present in the listed offset set"
    Nothing -> fail "The topic was not present in the assignment set"
  void $ liftIO $ leaveGroup kafka me
  wait <- liftIO $ registerDelay defaultTimeout
  void $ liftConsumer $ tryParse <$> getLeaveGroupResponse kafka wait

rejoin ::
     Kafka
  -> Topic
  -> TVar ConsumerState
  -> String
  -> Consumer [Int32]
rejoin kafka top currentState name = do
  let topicName = getTopicName top
  ConsumerState member _ _ <- liftIO $ readTVarIO currentState
  (newGenId, newMember, members) <- joinG kafka topicName member
  (newNewGenId, assigns) <- sync kafka top newMember members newGenId name
  case partitionsForTopic topicName assigns of
    Just indices -> do
      liftIO $ atomically $ modifyTVar' currentState
        (\state -> state
          { currentMember = newMember
          , currentGenId = newNewGenId
          })
      pure indices
    Nothing -> fail "The topic was not present in the assignment set"

partitionsForTopic :: TopicName -> [SyncTopicAssignment] -> Maybe [Int32]
partitionsForTopic (TopicName n) assigns =
  S.syncAssignedPartitions
  <$> find (\a -> S.syncAssignedTopic a == toByteString n) assigns

heartbeats :: Kafka -> TVar ConsumerState -> String -> IO ()
heartbeats kafka currentState name = do
  ConsumerState member genId _ <- readTVarIO currentState
  void $ heartbeat kafka member genId
  wait <- registerDelay fiveSeconds
  resp <- getHeartbeatResponse kafka wait
  atomically $ modifyTVar' currentState
    (\state -> state
      { currentHeartbeat = Just $ tryParse $
          (fmap . fmap) heartbeatErrorCode resp
      })
  threadDelay 500000
  heartbeats kafka currentState name

loop ::
     Kafka
  -> Topic
  -> TVar ConsumerState
  -> IntMap Int64
  -> [Int32]
  -> String
  -> Consumer ()
loop kafka top currentState offsets indices name = do
  let topicName = getTopicName top
  fetchResp <- getMessages kafka top offsets =<< liftIO (registerDelay 10000000)
  ConsumerState member genId heartbeatStatus <- liftIO (readTVarIO currentState)
  liftIO $ traverse_ B.putStrLn (fetchResponseContents fetchResp)
  newOffsets <- updateOffsets' kafka topicName member indices fetchResp
  void $ commitOffsets kafka topicName newOffsets member genId
  case heartbeatStatus of
    Just x -> case x of
      Right e | e == errorRebalanceInProgress -> do
        newIndices <- rejoin kafka top currentState name
        loop kafka top currentState newOffsets newIndices name
      Right e | e == noError -> do
        loop kafka top currentState newOffsets indices name
      Right e -> throwConsumer (KafkaUnexpectedErrorCodeException e)
      Left kafkaException -> throwConsumer kafkaException
    Nothing -> loop kafka top currentState newOffsets indices name

getMessages ::
     Kafka
  -> Topic
  -> IntMap Int64
  -> TVar Bool
  -> Consumer FetchResponse
getMessages kafka top offsets interrupt = do
  liftConsumer $ fetch kafka (getTopicName top) 5000000 (toOffsetList offsets)
  liftConsumer $ tryParse <$> getFetchResponse kafka interrupt

updateOffsets' ::
     Kafka
  -> TopicName
  -> GroupMember
  -> [Int32]
  -> FetchResponse
  -> Consumer (IntMap Int64)
updateOffsets' k topicName member partitionIndices r = do
  liftConsumer $ offsetFetch k member topicName partitionIndices
  offs <- liftConsumer $ tryParse <$> (O.getOffsetFetchResponse k =<< registerDelay defaultTimeout)
  case O.topics offs of
    [topicResponse] -> do
      let
        partitions = O.offsetFetchPartitions topicResponse
        fetchedOffsets = IM.fromList $
          fmap
            (\part ->
              (fromIntegral $ O.offsetFetchPartitionIndex part
              , O.offsetFetchOffset part))
            partitions
      pure (updateOffsets topicName fetchedOffsets r)
    _ -> fail
      ("Got unexpected number of topic responses: " <> show offs)

commitOffsets ::
     Kafka
  -> TopicName
  -> IntMap Int64
  -> GroupMember
  -> GenerationId
  -> Consumer ()
commitOffsets k topicName offs member genId = do
  liftConsumer $ offsetCommit k topicName (toOffsetList offs) member genId
  void $ liftConsumer $ tryParse <$> (C.getOffsetCommitResponse k =<< registerDelay defaultTimeout)

assignMembers :: Int -> Topic -> [Member] -> [MemberAssignment]
assignMembers memberCount top groupMembers =
  fmap (assignMember memberCount top) (zip groupMembers [0..])

assignMember :: Int -> Topic -> (Member, Int) -> MemberAssignment
assignMember memberCount top (member, i) =
  MemberAssignment
    memberName
    [TopicAssignment topicName assignments]
  where
  Topic topicName partitionCount _ = top
  memberName = fromByteString (J.groupMemberId member)
  assignments = fromIntegral <$>
    assignPartitions memberCount i partitionCount

assignPartitions :: Int -> Int -> Int -> [Int]
assignPartitions memberCount i partitionCount =
  [i, i + memberCount .. partitionCount-1]

noError :: Int16
noError = 0

errorUnknownMemberId :: Int16
errorUnknownMemberId = 25

errorRebalanceInProgress :: Int16
errorRebalanceInProgress = 27

errorMemberIdRequired :: Int16
errorMemberIdRequired = 79

expectedSyncErrors :: [Int16]
expectedSyncErrors =
  [ errorUnknownMemberId
  , errorRebalanceInProgress
  , errorMemberIdRequired
  ]

fetchResponseContents :: FetchResponse -> [ByteString]
fetchResponseContents fetchResponse =
    mapMaybe recordValue
  . concatMap F.records
  . concat
  . mapMaybe F.recordSet
  . concatMap F.partitionResponses
  . F.responses
  $ fetchResponse

sync ::
     Kafka
  -> Topic
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> String
  -> Consumer (GenerationId, [SyncTopicAssignment])
sync kafka top member members genId name = do
  let topicName = getTopicName top
      assignments = assignMembers (length members) top members
  liftConsumer $ syncGroup kafka member genId assignments
  wait <- liftIO (registerDelay defaultTimeout)
  sgr <- liftConsumer $ tryParse <$> S.getSyncGroupResponse kafka wait
  if S.errorCode sgr `elem` expectedSyncErrors then do
    (newGenId, newMember, newMembers) <- joinG kafka topicName member
    sync kafka top newMember newMembers newGenId name
  else if S.errorCode sgr == noError then
    pure (genId, fromMaybe [] $ S.partitionAssignments <$> S.memberAssignment sgr)
  else
    throwConsumer (KafkaUnexpectedErrorCodeException (S.errorCode sgr))

joinG ::
     Kafka
  -> TopicName
  -> GroupMember
  -> Consumer (GenerationId, GroupMember, [Member])
joinG kafka top member@(GroupMember name _) = do
  liftConsumer $ joinGroup kafka top member
  wait <- liftIO (registerDelay defaultTimeout)
  jgr <- liftConsumer $ tryParse <$> getJoinGroupResponse kafka wait
  if J.errorCode jgr == errorMemberIdRequired then do
    let memId = Just (fromByteString (J.memberId jgr))
    let assignment = GroupMember name memId
    joinG kafka top assignment
  else if J.errorCode jgr == noError then do
    let genId = GenerationId (J.generationId jgr)
    let memId = Just (fromByteString (J.memberId jgr))
    let assignment = GroupMember name memId
    pure (genId, assignment, J.members jgr)
  else
    throwConsumer (KafkaUnexpectedErrorCodeException (J.errorCode jgr))

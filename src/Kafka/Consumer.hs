{-# LANGUAGE LambdaCase #-}

module Kafka.Consumer
  ( consumerSession
  , merge
  ) where

import Control.Monad
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
merge lor ofr =
  IM.mergeWithKey
    (\_ l o -> Just $ if o < 0 then l else o)
    id
    id
    lor
    ofr

defaultTimeout :: Int
defaultTimeout = 30000000

fiveSeconds :: Int
fiveSeconds = 5000000

updateOffsets ::
     TopicName
  -> IntMap Int64
  -> FetchResponse
  -> IntMap Int64
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

consumerSession ::
     Kafka
  -> Topic
  -> GroupMember
  -> String
  -> IO (Either KafkaException ())
consumerSession kafka top oldMe name = do
  let topicName = getTopicName top
  joinG kafka topicName oldMe >>= \case
    Right (genId, me, members) -> do
      sync kafka top me members genId name >>= \case
        Right (newGenId, assigns) -> do
          case assigns of
            Nothing -> pure ()
            Just as -> do
              case partitionsForTopic topicName as of
                Just indices -> do
                  offs <- getInitialOffsets kafka me top indices
                  case offs of
                    Just initialOffsets -> do
                      heartbeatResult <- newTVarIO undefined
                      currentMember <- newTVarIO me
                      currentGenId <- newTVarIO newGenId
                      currentIndices <- newTVarIO indices
                      interruptFetch <- newTVarIO False
                      void $ forkIO $ withDefaultKafka $ \k ->
                        heartbeats
                          k
                          currentMember
                          currentGenId
                          name
                          heartbeatResult
                          interruptFetch
                      void $ loop
                        kafka
                        top
                        currentMember
                        initialOffsets
                        currentIndices
                        currentGenId
                        name
                        heartbeatResult
                        interruptFetch
                    Nothing -> fail "The topic was not present in the listed offset set"
                Nothing -> fail "The topic was not present in the assignment set"
          void $ leaveGroup kafka me
          wait <- registerDelay defaultTimeout
          void $ getLeaveGroupResponse kafka wait
          pure (Right ())
        Left e ->
          pure (Left e)
    Left e ->
      pure (Left e)

rejoin ::
     Kafka
  -> Topic
  -> TVar GroupMember
  -> TVar GenerationId
  -> TVar [Int32]
  -> String
  -> IO (Either KafkaException ())
rejoin kafka top currentMember currentGenId currentIndices name = do
  let topicName = getTopicName top
  member <- readTVarIO currentMember
  joinG kafka topicName member >>= \case
    Right (newGenId, newMember, members) -> do
      sync kafka top newMember members newGenId name >>= \case
        Right (newNewGenId, assigns) -> do
          case assigns of
            Nothing -> pure ()
            Just as -> do
              case partitionsForTopic topicName as of
                Just indices -> do
                  offs <- getInitialOffsets kafka newMember top indices
                  case offs of
                    Just _ -> do
                      atomically $ do
                        writeTVar currentMember newMember
                        writeTVar currentGenId newNewGenId
                        writeTVar currentIndices indices
                    Nothing -> fail "The topic was not present in the listed offset set"
                Nothing -> fail "The topic was not present in the assignment set"
          pure (Right ())
        Left e ->
          pure (Left e)
    Left e ->
      pure (Left e)

partitionsForTopic ::
     TopicName
  -> [SyncTopicAssignment]
  -> Maybe [Int32]
partitionsForTopic (TopicName n) assigns =
  S.syncAssignedPartitions
  <$> find (\a -> S.syncAssignedTopic a == toByteString n) assigns

heartbeats ::
     Kafka
  -> TVar GroupMember
  -> TVar GenerationId
  -> String
  -> TVar (Either KafkaException (Either String HeartbeatResponse))
  -> TVar Bool
  -> IO ()
heartbeats kafka me genId name heartbeatResult interruptFetch = do
  currentMember <- readTVarIO me
  currentGenId <- readTVarIO genId
  void $ heartbeat kafka currentMember currentGenId
  wait <- registerDelay fiveSeconds
  resp <- getHeartbeatResponse kafka wait
  atomically $ writeTVar heartbeatResult resp
  threadDelay 500000
  case (fmap . fmap) heartbeatErrorCode resp of
    Right (Right 0) -> atomically $ writeTVar interruptFetch False
    Right (Right _) -> atomically $ writeTVar interruptFetch True
    _ -> pure ()
  heartbeats kafka me genId name heartbeatResult interruptFetch

loop ::
     Kafka
  -> Topic
  -> TVar GroupMember
  -> IntMap Int64
  -> TVar [Int32]
  -> TVar GenerationId
  -> String
  -> TVar (Either KafkaException (Either String HeartbeatResponse))
  -> TVar Bool
  -> IO (Either KafkaException ())
loop kafka top currentMember offsets currentIndices currentGenId name handle interruptFetch = do
  let topicName = getTopicName top
  member <- readTVarIO currentMember
  genId <- readTVarIO currentGenId
  indices <- readTVarIO currentIndices
  fetchResp <- getMessages kafka top offsets =<< registerDelay 10000000
  case fetchResp of
    Right (Right r) -> do
      traverse_ B.putStrLn (fetchResponseContents r)
      newOffsets <- either (const offsets) id <$>
        updateOffsets' kafka topicName member indices r
      void $ commitOffsets kafka topicName newOffsets member genId
      heartbeatStatus <- readTVarIO handle
      case (fmap . fmap) heartbeatErrorCode heartbeatStatus of
        Right (Right e) | e == errorRebalanceInProgress -> do
          void $ rejoin kafka top currentMember currentGenId currentIndices name
          loop kafka top currentMember newOffsets currentIndices currentGenId name handle interruptFetch
        Right (Right 0) -> do
          loop kafka top currentMember newOffsets currentIndices currentGenId name handle interruptFetch
        Right (Right e) -> pure (Left (KafkaUnexpectedErrorCodeException e))
        Right (Left parseError) -> pure (Left (KafkaParseException parseError))
        Left netError -> pure (Left netError)
    Right (Left parseError) -> pure (Left (KafkaParseException parseError))
    Left e -> do
      pure (Left e)

getMessages ::
     Kafka
  -> Topic
  -> IntMap Int64
  -> TVar Bool
  -> IO (Either KafkaException (Either String FetchResponse))
getMessages kafka top offsets interrupt = do
  void $ fetch kafka (getTopicName top) 5000000 (toOffsetList offsets)
  getFetchResponse kafka interrupt

updateOffsets' ::
     Kafka
  -> TopicName
  -> GroupMember
  -> [Int32]
  -> FetchResponse
  -> IO (Either KafkaException (IntMap Int64))
updateOffsets' k topicName member partitionIndices r = do
  void $ offsetFetch k member topicName partitionIndices
  offsets <- O.getOffsetFetchResponse k =<< registerDelay defaultTimeout
  case offsets of
    Right (Right offs) -> do
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
          pure (Right (updateOffsets topicName fetchedOffsets r))
        _ -> fail
          ("Got unexpected number of topic responses: " <> show offs)
    Right (Left parseError) -> pure (Left (KafkaParseException parseError))
    Left networkError -> pure (Left networkError)

commitOffsets ::
     Kafka
  -> TopicName
  -> IntMap Int64
  -> GroupMember
  -> GenerationId
  -> IO (Either KafkaException ())
commitOffsets k topicName offs member genId = do
  void $ offsetCommit k topicName (toOffsetList offs) member genId
  commitResponse <- C.getOffsetCommitResponse k =<< registerDelay defaultTimeout
  case commitResponse of
    Right (Right _) -> pure (Right ())
    Right (Left parseError) -> pure (Left (KafkaParseException parseError))
    Left networkError -> pure (Left networkError)

assignMembers :: Int -> Topic -> [Member] -> [MemberAssignment]
assignMembers memberCount top groupMembers =
  fmap
    (assignMember memberCount top)
    (zip groupMembers [0..])

assignMember :: Int -> Topic -> (Member, Int) -> MemberAssignment
assignMember memberCount top (member, i) =
  MemberAssignment (fromByteString $ J.groupMemberId member)
    [TopicAssignment
      topicName
      (fromIntegral <$> assignPartitions memberCount i partitionCount)]
  where
  Topic topicName partitionCount _ = top

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
  -> IO (Either KafkaException (GenerationId, Maybe [SyncTopicAssignment]))
sync kafka top member members genId name = do
  let topicName = getTopicName top
      assignments = assignMembers (length members) top members
  void $ syncGroup kafka member genId assignments
  wait <- registerDelay defaultTimeout
  S.getSyncGroupResponse kafka wait >>= \case
    Right (Right sgr) | S.errorCode sgr `elem` expectedSyncErrors -> do
      joinG kafka topicName member >>= \case
        Right (newGenId, newMember, newMembers) ->
          sync kafka top newMember newMembers newGenId name
        Left e -> pure (Left e)
    Right (Right sgr) | S.errorCode sgr == noError -> do
      pure (Right (genId, S.partitionAssignments <$> S.memberAssignment sgr))
    Right (Right sgr) -> do
      pure (Left (KafkaUnexpectedErrorCodeException (S.errorCode sgr)))
    Right (Left parseError) -> pure (Left (KafkaParseException parseError))
    Left networkError -> pure (Left networkError)

joinG ::
     Kafka
  -> TopicName
  -> GroupMember
  -> IO (Either KafkaException (GenerationId, GroupMember, [Member]))
joinG kafka top member@(GroupMember name _) = do
  void $ joinGroup kafka top member
  wait <- registerDelay defaultTimeout
  getJoinGroupResponse kafka wait >>= \case
    Right (Right jgr) | J.errorCode jgr == errorMemberIdRequired -> do
      let memId = Just (fromByteString (J.memberId jgr))
      let assignment = GroupMember name memId
      joinG kafka top assignment
    Right (Right jgr) | J.errorCode jgr == noError -> do
      let genId = GenerationId (J.generationId jgr)
      let memId = Just (fromByteString (J.memberId jgr))
      let assignment = GroupMember name memId
      pure (Right (genId, assignment, J.members jgr))
    Right (Right jgr) ->
      pure (Left (KafkaUnexpectedErrorCodeException (J.errorCode jgr)))
    Right (Left parseError) -> pure (Left (KafkaParseException parseError))
    Left networkError -> pure (Left networkError)

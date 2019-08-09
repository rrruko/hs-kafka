{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Consumer
  ( consumerSession
  , merge
  ) where

import Chronos
import Control.Concurrent.STM.TVar
import Control.Concurrent.MVar
import Control.Monad hiding (join)
import Control.Monad.Except hiding (join)
import Data.Foldable
import Data.Int
import Data.IntMap (IntMap)
import Data.Maybe
import GHC.Conc (forkIO, atomically)
import Torsor

import qualified Data.IntMap as IM

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.Heartbeat.Response (getHeartbeatResponse)
import Kafka.JoinGroup.Response (Member, getJoinGroupResponse)
import Kafka.LeaveGroup.Response
import Kafka.ListOffsets.Response (ListOffsetsResponse)
import Kafka.SyncGroup.Response (SyncTopicAssignment)

import qualified Kafka.Fetch.Response as F
import qualified Kafka.JoinGroup.Response as J
import qualified Kafka.ListOffsets.Response as L
import qualified Kafka.OffsetCommit.Response as C
import qualified Kafka.OffsetFetch.Response as O
import qualified Kafka.SyncGroup.Response as S

-- | This module provides a high-level interface to the Kafka API for
-- consumers by wrapping the low-level request and response type modules.

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

getListedOffsets ::
     Kafka
  -> Topic
  -> [Int32]
  -> Consumer (IntMap Int64)
getListedOffsets kafka topic indices = do
  let Topic name _ _ = topic
  let topicName = TopicName name
  liftConsumer $ listOffsets kafka topicName indices Earliest
  listOffsetsTimeout <- liftIO (registerDelay fiveSeconds)
  listedOffs <- liftConsumer $ tryParse <$>
    L.getListOffsetsResponse kafka listOffsetsTimeout
  pure (listOffsetsMap listedOffs)

initializeOffsets ::
     Kafka
  -> Topic
  -> GroupMember
  -> GenerationId
  -> Consumer ()
initializeOffsets kafka topic member genId = do
  let Topic name' partitionCount _ = topic
      topicName = TopicName name'
      allIndices = [0..fromIntegral partitionCount - 1]
  initialOffs <- getListedOffsets kafka topic allIndices
  latestOffs <- latestOffsets kafka topicName member allIndices
  let validOffs = merge initialOffs latestOffs
  commitOffsets kafka topicName validOffs member genId

merge :: IntMap Int64 -> IntMap Int64 -> IntMap Int64
merge lor ofr = mergeId (\l o -> Just $ if o < 0 then l else o) lor ofr
  where
  mergeId f a b = IM.mergeWithKey (\_ left right -> f left right) id id a b

toOffsetList :: IntMap Int64 -> [PartitionOffset]
toOffsetList = map (\(k, v) -> PartitionOffset (fromIntegral k) v) . IM.toList

listOffsetsMap :: ListOffsetsResponse -> IntMap Int64
listOffsetsMap lor = IM.fromList $ map
  (\pr -> (fromIntegral (L.partition pr), L.offset pr))
  (concatMap L.partitionResponses $ L.responses lor)

defaultTimeout :: Int
defaultTimeout = 30000000

fiveSeconds :: Int
fiveSeconds = 5000000

partitionHighWatermark :: TopicName -> FetchResponse -> Int32 -> Maybe Int64
partitionHighWatermark (TopicName topicName) fetchResponse partitionId =
  highWatermark . partitionHeader <$> getPartitionResponse partitionId
  where
  topicResps = filter
    (\x -> F.fetchResponseTopic x == toByteString topicName)
    (F.responses fetchResponse)
  partitionResps = concatMap partitionResponses topicResps
  getPartitionResponse pid =
    find
      (\resp -> pid == partition (partitionHeader resp))
      partitionResps

updateOffsets :: TopicName -> IntMap Int64 -> FetchResponse -> IntMap Int64
updateOffsets topicName current r =
  IM.mapWithKey
    (\pid offs -> fromMaybe offs
      (partitionHighWatermark topicName r (fromIntegral pid)))
    current

data ConsumerState = ConsumerState
  { currentMember :: GroupMember
  , currentGenId :: GenerationId
  , currentAssignments :: [Int32]
  , lastRequestTime :: Time
  } deriving (Show)

-- | Consume on a topic until terminated. Takes a callback parameter that will
-- be called when a response is received from Kafka. Responses might be empty.
consumerSession ::
     Kafka
  -> Topic
  -> GroupMember
  -> (FetchResponse -> IO ())
  -> TVar Bool
  -> IO (Either KafkaException ())
consumerSession kafka top oldMe callback leave = runExceptT $ runConsumer $ do
  let topicName = getTopicName top
  (genId, me, members) <- join kafka topicName oldMe
  (newGenId, assigns) <- sync kafka top me members genId
  initializeOffsets kafka top me newGenId
  case partitionsForTopic topicName assigns of
    Just indices -> do
      let initialState = ConsumerState me newGenId indices epoch
      currentState <- liftIO $ newTVarIO initialState
      sock <- liftIO $ newMVar 0
      let runHeartbeats k = heartbeats k top currentState leave sock
          runFetches k = doFetches k top currentState callback leave sock
      void . liftIO . forkIO . void . runExceptT . runConsumer $ runFetches kafka
      void . liftIO $ runHeartbeats kafka
    Nothing -> fail "The topic was not present in the assignment set"

-- | rejoin is called when the client receives a "rebalance in progress"
-- error code, triggered by another client joining or leaving the group.
-- It sends join and sync requests and receives a new member id, generation
-- id, and set of assigned topics.
rejoin ::
     Kafka
  -> Topic
  -> TVar ConsumerState
  -> Consumer [Int32]
rejoin kafka top currentState = do
  let topicName = getTopicName top
  ConsumerState member _ _ _ <- liftIO $ readTVarIO currentState
  (genId, newMember, members) <- join kafka topicName member
  (newGenId, assigns) <- sync kafka top newMember members genId
  case partitionsForTopic topicName assigns of
    Just indices -> do
      liftIO $ atomically $ modifyTVar' currentState
        (\state -> state
          { currentMember = newMember
          , currentGenId = newGenId
          , currentAssignments = indices
          })
      pure indices
    Nothing -> fail "The topic was not present in the assignment set"

partitionsForTopic :: TopicName -> [SyncTopicAssignment] -> Maybe [Int32]
partitionsForTopic (TopicName n) assigns =
  S.syncAssignedPartitions
  <$> find (\a -> S.syncAssignedTopic a == toByteString n) assigns

-- | A client in a consumer group has to send heartbeats. Starting a session
-- spawns a thread which calls this function to manage heartbeats and respond
-- to error codes in the heartbeat response. In particular, if a "rebalance in
-- progress" error is received, the client will rejoin the group.
heartbeats :: Kafka -> Topic -> TVar ConsumerState -> TVar Bool -> MVar Int16 -> IO ()
heartbeats kafka top currentState leave sock = do
  ConsumerState member genId _ lastReqTime <- readTVarIO currentState
  withMVar sock $ \case
    e | e == errorRebalanceInProgress -> do
      void $ runExceptT $ runConsumer $ rejoin kafka top currentState
    e | e == noError ->
      pure ()
    e ->
      fail ("Unknown error code " <> show e)
  l <- liftIO $ readTVarIO leave
  now' <- now
  when (difference now' lastReqTime > scale 5 second) $ do
    void $ heartbeat kafka member genId
    timeout <- registerDelay fiveSeconds
    void $ getHeartbeatResponse kafka timeout
    n <- now
    atomically $ modifyTVar' currentState (\cs -> cs { lastRequestTime = n })
  if l then do
    void $ leaveGroup kafka member
    timeout <- registerDelay defaultTimeout
    void $ getLeaveGroupResponse kafka timeout
  else do
    heartbeats kafka top currentState leave sock

-- | Repeatedly fetch messages from kafka and commit the new offsets.
-- Read any updates that have been made to the consumer state by the
-- heartbeats thread.
doFetches ::
     Kafka
  -> Topic
  -> TVar ConsumerState
  -> (FetchResponse -> IO ())
  -> TVar Bool
  -> MVar Int16
  -> Consumer ()
doFetches kafka top currentState callback leave sock = do
  let topicName = getTopicName top
  neverInterrupt <- liftIO $ newTVarIO False
  liftConsumer $ modifyMVar sock $ \err -> 
    updateError err $ runExceptT $ runConsumer $ do
      ConsumerState member genId indices _ <- liftIO (readTVarIO currentState)
      latestOffs <- latestOffsets kafka topicName member indices
      fetchResp <- getMessages kafka top latestOffs neverInterrupt
      liftIO $ do
        n <- now
        atomically $ modifyTVar' currentState (\cs -> cs { lastRequestTime = n })
      liftIO $ callback fetchResp
      newOffsets <- updateOffsets' kafka topicName member indices fetchResp
      void $ commitOffsets kafka topicName newOffsets member genId
      pure (F.errorCode fetchResp)
  l <- liftIO $ readTVarIO leave
  when (not l) $
    doFetches kafka top currentState callback leave sock
  where
  updateError err = fmap $ \case
    Left e -> (err, Left e)
    Right i -> (i, Right ())

getMessages ::
     Kafka
  -> Topic
  -> IntMap Int64
  -> TVar Bool
  -> Consumer FetchResponse
getMessages kafka top offsets interrupt = do
  liftConsumer $ fetch kafka (getTopicName top) 1000000 offsetList
  liftConsumer $ tryParse <$> getFetchResponse kafka interrupt
  where
  offsetList = toOffsetList offsets

updateOffsets' ::
     Kafka
  -> TopicName
  -> GroupMember
  -> [Int32]
  -> FetchResponse
  -> Consumer (IntMap Int64)
updateOffsets' k topicName member partitionIndices r = do
  fetchedOffsets <- latestOffsets k topicName member partitionIndices
  pure (updateOffsets topicName fetchedOffsets r)

latestOffsets ::
     Kafka
  -> TopicName
  -> GroupMember
  -> [Int32]
  -> Consumer (IntMap Int64)
latestOffsets kafka topicName member indices = do
  liftConsumer $ offsetFetch kafka member topicName indices
  timeout <- liftIO $ registerDelay fiveSeconds
  offs <- liftConsumer $ tryParse <$> O.getOffsetFetchResponse kafka timeout
  pure (offsetFetchOffsets offs)

offsetFetchOffsets :: O.OffsetFetchResponse -> IntMap Int64
offsetFetchOffsets ofr = IM.fromList $ fmap
  (\part ->
    (fromIntegral $ O.offsetFetchPartitionIndex part
      , O.offsetFetchOffset part))
    (concatMap O.offsetFetchPartitions $ O.topics ofr)

commitOffsets ::
     Kafka
  -> TopicName
  -> IntMap Int64
  -> GroupMember
  -> GenerationId
  -> Consumer ()
commitOffsets k topicName offs member genId = do
  liftConsumer $ offsetCommit k topicName (toOffsetList offs) member genId
  timeout <- liftIO $ registerDelay fiveSeconds
  void $ liftConsumer $ tryParse <$> C.getOffsetCommitResponse k timeout

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

sync ::
     Kafka
  -> Topic
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> Consumer (GenerationId, [SyncTopicAssignment])
sync kafka top member members genId = do
  let topicName = getTopicName top
      assignments = assignMembers (length members) top members
  liftConsumer $ syncGroup kafka member genId assignments
  wait <- liftIO (registerDelay defaultTimeout)
  sgr <- liftConsumer $ tryParse <$> S.getSyncGroupResponse kafka wait
  if S.errorCode sgr `elem` expectedSyncErrors then do
    (newGenId, newMember, newMembers) <- join kafka topicName member
    sync kafka top newMember newMembers newGenId
  else if S.errorCode sgr == noError then do
    let assigns = S.partitionAssignments <$> S.memberAssignment sgr
    pure (genId, fromMaybe [] assigns)
  else
    throwConsumer (KafkaUnexpectedErrorCodeException (S.errorCode sgr))

join ::
     Kafka
  -> TopicName
  -> GroupMember
  -> Consumer (GenerationId, GroupMember, [Member])
join kafka top member@(GroupMember name _) = do
  liftConsumer $ joinGroup kafka top member
  wait <- liftIO (registerDelay defaultTimeout)
  jgr <- liftConsumer $ tryParse <$> getJoinGroupResponse kafka wait
  if J.errorCode jgr == errorMemberIdRequired then do
    let memId = Just (fromByteString (J.memberId jgr))
    let assignment = GroupMember name memId
    join kafka top assignment
  else if J.errorCode jgr == noError then do
    let genId = GenerationId (J.generationId jgr)
    let memId = Just (fromByteString (J.memberId jgr))
    let assignment = GroupMember name memId
    pure (genId, assignment, J.members jgr)
  else
    throwConsumer (KafkaUnexpectedErrorCodeException (J.errorCode jgr))

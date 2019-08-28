{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UndecidableInstances #-}

module Kafka.Consumer
  ( AutoCommit(..)
  , Consumer(..)
  , ConsumerSettings(..)
  , ConsumerState(..)
  , commitOffsets
  , evalConsumer
  , getRecordSet
  , leave
  , merge
  , newConsumer
  , getsv

  , FetchResponse(..)
  , FetchTopic(..)
  , Header(..)
  , PartitionHeader(..)
  , FetchPartition(..)
  , Record(..)
  , RecordBatch(..)
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad hiding (join)
import Control.Monad.Except hiding (join)
import Control.Monad.Reader hiding (join)
import Data.Coerce
import Data.Foldable
import Data.Primitive.ByteArray
import Data.Int
import Data.IntMap (IntMap)
import Data.Maybe

import qualified Data.IntMap as IM

import Kafka.Common
import Kafka.Internal.Request
import Kafka.Internal.Fetch.Response
import Kafka.Internal.JoinGroup.Response (Member, getJoinGroupResponse)
import Kafka.Internal.LeaveGroup.Response
import Kafka.Internal.Heartbeat.Response (getHeartbeatResponse)
import Kafka.Internal.ListOffsets.Response (ListOffsetsResponse)
import Kafka.Internal.SyncGroup.Response (SyncTopicAssignment)
import Kafka.Topic

import qualified Kafka.Internal.Fetch.Response as F
import qualified Kafka.Internal.Heartbeat.Response as H
import qualified Kafka.Internal.JoinGroup.Response as J
import qualified Kafka.Internal.ListOffsets.Response as L
import qualified Kafka.Internal.OffsetCommit.Response as C
import qualified Kafka.Internal.OffsetFetch.Response as O
import qualified Kafka.Internal.SyncGroup.Response as S

-- | This module provides a high-level interface to the Kafka API for
-- consumers by wrapping the low-level request and response type modules.
newtype Consumer a
  = Consumer
      { runConsumer ::
          ReaderT (TVar ConsumerState) (ExceptT KafkaException IO) a
      }
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadError KafkaException
    , MonadReader (TVar ConsumerState)
    )

tryParse :: Either KafkaException (Either String a) -> Either KafkaException a
tryParse = \case
  Right (Right parsed) -> Right parsed
  Right (Left parseError) -> Left (KafkaParseException parseError)
  Left networkError -> Left networkError

instance MonadIO Consumer where
  liftIO = Consumer . liftIO

getv :: Consumer ConsumerState
getv = Consumer $ do
  v <- ask
  currVal <- liftIO (readTVarIO v)
  pure currVal

getsv :: (ConsumerState -> a) -> Consumer a
getsv f = fmap f getv

modifyv :: (ConsumerState -> ConsumerState) -> Consumer ()
modifyv f = Consumer $ do
  v <- ask
  liftIO $ atomically $ modifyTVar' v f

liftConsumer :: IO (Either KafkaException a) -> Consumer a
liftConsumer = Consumer . lift . ExceptT

evalConsumer ::
     TVar ConsumerState
  -> Consumer a
  -> IO (Either KafkaException a)
evalConsumer v consumer = do
  (runExceptT . flip runReaderT v . runConsumer) consumer

-- | Configuration determined before the consumer starts
data ConsumerSettings = ConsumerSettings
  { groupFetchStart :: !KafkaTimestamp -- ^ Where to start if the group is new
  , maxFetchBytes :: !Int32 -- ^ Maximum number of bytes to allow per response
  , csTopicName :: !TopicName -- ^ Topic to fetch on
  , groupName :: !ByteArray -- ^ Name of the group
  , defaultTimeout :: !Int
  , autoCommit :: !AutoCommit
  } deriving (Show)

data AutoCommit = AutoCommit | NoAutoCommit
  deriving (Eq, Show)

-- | Consumer information that varies while the consumer is running
data ConsumerState = ConsumerState
  { kafka :: !Kafka
  , settings :: !ConsumerSettings
  , member :: !GroupMember
  , genId :: !GenerationId
  , members :: [Member]
  , offsets :: !(IntMap Int64)
  , partitionCount :: !Int32
  , sock :: !KafkaSocket
  , quit :: !Interruptedness
  } deriving (Show)

newtype KafkaSocket = KafkaSocket { getSock :: MVar () }

putKafkaSocket :: KafkaSocket -> () -> IO ()
putKafkaSocket = putMVar . getSock

takeKafkaSocket :: KafkaSocket -> IO ()
takeKafkaSocket = takeMVar . getSock

withSocket :: KafkaSocket -> Consumer a -> Consumer a
withSocket sock consumer = Consumer $ ReaderT $ \r -> ExceptT $ do
  liftIO (takeKafkaSocket sock)
  result <- evalConsumer r consumer
  liftIO (putKafkaSocket sock ())
  pure result

instance Show KafkaSocket where
  show _ = "<socket>"

getListedOffsets :: [Int32] -> Consumer (IntMap Int64)
getListedOffsets allIndices = do
  ConsumerState {..} <- getv
  let ConsumerSettings {..} = settings
  liftConsumer $ listOffsets kafka csTopicName allIndices groupFetchStart
  listOffsetsTimeout <- liftIO (registerDelay defaultTimeout)
  listedOffs <- liftConsumer $ tryParse <$>
    L.getListOffsetsResponse kafka listOffsetsTimeout
  pure (listOffsetsMap listedOffs)

initializeOffsets :: [Int32] -> Consumer ()
initializeOffsets assignedPartitions = do
  ConsumerState {..} <- getv
  let ConsumerSettings {..} = settings
  withSocket sock $ do
    initialOffs <- getListedOffsets assignedPartitions
    latestOffs <- latestOffsets assignedPartitions
    let validOffs = merge initialOffs latestOffs
    modifyv (\s -> s { offsets = validOffs })
    getv >>= commitOffsets'

merge :: IntMap Int64 -> IntMap Int64 -> IntMap Int64
merge lor ofr = mergeId (\l o -> Just $ if o < 0 then l else o) lor ofr
  where
  mergeId f a b = IM.mergeWithKey (\_ left right -> f left right) id id a b

toOffsetList :: IntMap Int64 -> [PartitionOffset]
toOffsetList = map (\(k, v) -> PartitionOffset (fromIntegral k) v) . IM.toList

listOffsetsMap :: ListOffsetsResponse -> IntMap Int64
listOffsetsMap lor = IM.fromList $ map
  (\pr -> (fromIntegral (L.partition pr), L.offset pr))
  (concatMap L.partitions $ L.topics lor)

updateOffsets :: TopicName -> IntMap Int64 -> FetchResponse -> IntMap Int64
updateOffsets topicName current r =
  IM.mapWithKey
    (\pid offs -> fromMaybe offs
      (F.partitionLastSeenOffset r name (fromIntegral pid)))
    current
  where
  name = toByteString (coerce topicName)

newConsumer ::
     Kafka
  -> ConsumerSettings
  -> IO (Either KafkaException (TVar ConsumerState))
newConsumer kafka settings@(ConsumerSettings {..}) = runExceptT $ do
  let initialMember = GroupMember groupName Nothing
  partitionCount <- ExceptT $ getPartitionCount kafka csTopicName defaultTimeout
  (genId', me, members) <- join kafka csTopicName initialMember
  (newGenId, assigns) <- sync kafka csTopicName partitionCount me members genId'
  case partitionsForTopic csTopicName assigns of
    Just indices -> do
      so <- liftIO (newMVar ())
      let initialState = ConsumerState
            { settings = settings
            , genId = newGenId
            , kafka = kafka
            , member = me
            , members = members
            , offsets = mempty
            , partitionCount = partitionCount
            , sock = KafkaSocket so
            , quit = Uninterrupted
            }
      v <- liftIO (newTVarIO initialState)
      void . liftIO . forkIO $ void $ evalConsumer v heartbeats
      flip runReaderT v (runConsumer (initializeOffsets indices))
      pure v
    Nothing -> throwError $ KafkaException
      "The topic was not present in the assignment set"

heartbeats :: Consumer ()
heartbeats = do
  getsv quit >>= \case
    Interrupted -> pure ()
    Uninterrupted -> do
      liftIO (threadDelay 3000000)
      void sendHeartbeat
      heartbeats

-- | rejoin is called when the client receives a "rebalance in progress"
-- error code, triggered by another client joining or leaving the group.
-- It sends join and sync requests and receives a new member id, generation
-- id, and set of assigned topics.
rejoin :: Consumer ()
rejoin = do
  ConsumerState {kafka, member, partitionCount, settings} <- getv
  let topicName = csTopicName settings
  (genId', newMember, members') <- Consumer $ lift $ join kafka topicName member
  (newGenId, assigns) <- Consumer $ lift $ sync kafka topicName partitionCount newMember members' genId'
  case partitionsForTopic topicName assigns of
    Just indices -> do
      newOffsets <- latestOffsets indices
      modifyv $ \s -> s
        { member = newMember
        , genId = newGenId
        , offsets = newOffsets
        }
    Nothing -> throwError $ KafkaException
      "The topic was not present in the assignment set"

partitionsForTopic :: TopicName -> [SyncTopicAssignment] -> Maybe [Int32]
partitionsForTopic (TopicName n) assigns =
  S.partitions <$> find (\a -> S.topic a == toByteString n) assigns

sendHeartbeat :: Consumer ()
sendHeartbeat = do
  ConsumerState {sock, member, genId, kafka, settings} <- getv
  withSocket sock $ do
    liftConsumer $ heartbeat kafka member genId
    timeout <- liftIO $ registerDelay (defaultTimeout settings)
    resp <- liftConsumer $ tryParse <$> getHeartbeatResponse kafka timeout
    when (H.errorCode resp == errorRebalanceInProgress) rejoin

leave :: Consumer ()
leave = do
  ConsumerState {..} <- getv
  withSocket sock $ do
    liftConsumer $ leaveGroup kafka member
    timeout <- liftIO $ registerDelay (defaultTimeout settings)
    void $ liftConsumer $ tryParse <$> getLeaveGroupResponse kafka timeout
    modifyv (\s -> s { quit = Interrupted })

getRecordSet :: Int -> Consumer FetchResponse
getRecordSet fetchWaitTime = do
  getsv sock >>= \so -> withSocket so $ do
    ConsumerState {..} <- getv
    let offsetList = toOffsetList offsets
        ConsumerSettings {..} = settings
    liftConsumer $ fetch kafka csTopicName fetchWaitTime offsetList maxFetchBytes
    interrupt <- liftIO $ registerDelay defaultTimeout
    fetchResp <- liftConsumer $ tryParse <$> getFetchResponse kafka interrupt
    let newOffsets = updateOffsets csTopicName offsets fetchResp
    modifyv (\s -> s { offsets = newOffsets })
    cs <- getv
    when (autoCommit == AutoCommit) (commitOffsets' cs)
    pure fetchResp

latestOffsets :: [Int32] -> Consumer (IntMap Int64)
latestOffsets indices = do
  ConsumerState {..} <- getv
  liftConsumer $ offsetFetch kafka member (csTopicName settings) indices
  timeout <- liftIO $ registerDelay (defaultTimeout settings)
  offs <- liftConsumer $ tryParse <$> O.getOffsetFetchResponse kafka timeout
  pure (offsetFetchOffsets offs)

offsetFetchOffsets :: O.OffsetFetchResponse -> IntMap Int64
offsetFetchOffsets ofr = IM.fromList $ fmap
  (\part -> (fromIntegral $ O.partitionIndex part, O.offset part))
  (concatMap O.partitions $ O.topics ofr)

commitOffsets' :: ConsumerState -> Consumer ()
commitOffsets' cs = do
  let ConsumerState {..} = cs
  let ConsumerSettings {..} = settings
  liftConsumer $ offsetCommit kafka csTopicName (toOffsetList offsets) member genId
  timeout <- liftIO $ registerDelay defaultTimeout
  void $ liftConsumer $ tryParse <$> C.getOffsetCommitResponse kafka timeout

commitOffsets :: Consumer ()
commitOffsets = do
  cs@ConsumerState {..} <- getv
  withSocket sock (commitOffsets' cs)

assignMembers :: Int -> TopicName -> Int32 -> [Member] -> [MemberAssignment]
assignMembers memberCount topicName partitionCount groupMembers =
  fmap (assignMember memberCount topicName partitionCount) (zip groupMembers [0..])

assignMember :: Int -> TopicName -> Int32 -> (Member, Int) -> MemberAssignment
assignMember memberCount topicName partitionCount (member, i) =
  MemberAssignment
    memberName
    [TopicAssignment (coerce topicName) assignments]
  where
  memberName = fromByteString (J.groupMemberId member)
  assignments = fromIntegral <$>
    assignPartitions memberCount i partitionCount

assignPartitions :: Int -> Int -> Int32 -> [Int]
assignPartitions memberCount i partitionCount =
  [i, i + memberCount .. fromIntegral partitionCount-1]

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

-- We want to wait a long time for a response when we join the group because
-- the server may be waiting on members from a previous generation who didn't
-- leave properly
joinTimeout :: Int
joinTimeout = 30000000

sync ::
     Kafka
  -> TopicName
  -> Int32
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> ExceptT KafkaException IO (GenerationId, [SyncTopicAssignment])
sync kafka topicName partitionCount member members genId = do
  let assignments = assignMembers (length members) topicName partitionCount members
  ExceptT $ syncGroup kafka member genId assignments
  wait <- liftIO (registerDelay joinTimeout)
  sgr <- ExceptT $ tryParse <$> S.getSyncGroupResponse kafka wait
  if S.errorCode sgr `elem` expectedSyncErrors then do
    (newGenId, newMember, newMembers) <- join kafka topicName member
    sync kafka topicName partitionCount newMember newMembers newGenId
  else if S.errorCode sgr == noError then do
    let assigns = S.partitionAssignments <$> S.memberAssignment sgr
    pure (genId, fromMaybe [] assigns)
  else
    throwError (KafkaUnexpectedErrorCodeException (S.errorCode sgr))

join ::
     Kafka
  -> TopicName
  -> GroupMember
  -> ExceptT KafkaException IO (GenerationId, GroupMember, [Member])
join kafka top member@(GroupMember name _) = do
  ExceptT $ joinGroup kafka top member
  wait <- liftIO (registerDelay joinTimeout)
  jgr <- ExceptT $ tryParse <$> getJoinGroupResponse kafka wait
  if J.errorCode jgr == errorMemberIdRequired then do
    let memId = Just (fromByteString (J.memberId jgr))
        assignment = GroupMember name memId
    join kafka top assignment
  else if J.errorCode jgr == noError then do
    let genId = GenerationId (J.generationId jgr)
        memId = Just (fromByteString (J.memberId jgr))
        assignment = GroupMember name memId
    pure (genId, assignment, J.members jgr)
  else
    throwError (KafkaUnexpectedErrorCodeException (J.errorCode jgr))

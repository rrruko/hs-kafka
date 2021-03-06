{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
  , withConsumer
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
import Control.Monad hiding (join)
import Control.Monad.Except hiding (join)
import Control.Monad.Reader hiding (join)
import Data.Foldable
import Data.IntMap (IntMap)
import qualified Data.List as List
import Data.Maybe
import Socket.Stream.IPv4 (Peer)

import qualified Data.IntMap as IM

import Kafka.Common
import Kafka.Internal.Request
import Kafka.Internal.Request.Types
import Kafka.Internal.Fetch.Response
import Kafka.Internal.JoinGroup.Response (Member, getJoinGroupResponse)
import Kafka.Internal.FindCoordinator.Response (getFindCoordinatorResponse)
import Kafka.Internal.LeaveGroup.Response (getLeaveGroupResponse)
import Kafka.Internal.Heartbeat.Response (getHeartbeatResponse)
import Kafka.Internal.ListOffsets.Response (ListOffsetsResponse)
import Kafka.Internal.SyncGroup.Response (SyncTopicAssignment)
import Kafka.Internal.Topic

import qualified Kafka.Internal.LeaveGroup.Response as LeaveGroup
import qualified Kafka.Internal.Fetch.Response as F
import qualified Kafka.Internal.Heartbeat.Response as H
import qualified Kafka.Internal.JoinGroup.Response as J
import qualified Kafka.Internal.ListOffsets.Response as L
import qualified Kafka.Internal.OffsetCommit.Response as C
import qualified Kafka.Internal.OffsetFetch.Response as O
import qualified Kafka.Internal.SyncGroup.Response as S

import qualified String.Ascii as Str

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
  { groupFetchStart :: !KafkaTimestamp
    -- ^ Where to start if the group is new
  , maxFetchBytes :: !Int32
    -- ^ Maximum number of bytes to allow per response
  , csTopicName :: !TopicName
    -- ^ Topic to fetch on
  , groupName :: !GroupName
    -- ^ Name of the group
  , timeout :: !Int
    -- ^ Timeout for polling from Kafka, in microseconds
  , autoCommit :: !AutoCommit
    -- ^ Whether or not to automatically commit offsets
  , handle :: !(Maybe Handle)
    -- ^ Handle for writing debug logs
  }

-- | Whether or not to automatically commit offsets
data AutoCommit
  = AutoCommit -- ^ Automatically commit offsets
  | NoAutoCommit -- ^ Don't automatically commit offsets
  deriving (Eq, Show)

-- | Consumer information that varies while the consumer is running
data ConsumerState = ConsumerState
  { kafka :: !Kafka
    -- ^ Connection to Kafka
  , settings :: !ConsumerSettings
    -- ^ Settings fro the Consumer
  , member :: !GroupMember
    -- ^ Member Id (IMPROVE)
  , genId :: !GenerationId
    -- ^ Generation Id (IMPROVE) -- improve how?
  , members :: [Member]
    -- ^ Members (IMPROVE) -- improve how?
  , offsets :: !(IntMap Int64)
    -- ^ Offsets according to each partition
  , partitionCount :: !Int32
    -- ^ Number of partitions
  , sock :: !(MVar ())
    -- ^ KafkaSocket (IMPROVE) -- improve how?
  , quit :: !Interruptedness
    -- ^ Whether or not consumption has been interrupted.
  }

withSocket :: MVar () -> Consumer a -> Consumer a
withSocket sock consumer = Consumer $ ReaderT $ \r -> ExceptT $ do
  liftIO (takeMVar sock)
  result <- evalConsumer r consumer
  liftIO (putMVar sock ())
  pure result

getListedOffsets :: [Int32] -> Consumer (IntMap Int64)
getListedOffsets allIndices = do
  ConsumerState {..} <- getv
  let ConsumerSettings {..} = settings
  liftConsumer $ listOffsets kafka (ListOffsetsRequest csTopicName allIndices groupFetchStart) handle
  listOffsetsTimeout <- liftIO (registerDelay timeout)
  listedOffs <- liftConsumer $ tryParse <$>
    L.getListOffsetsResponse kafka listOffsetsTimeout handle
  let lots = L.topics listedOffs
  let errs = case List.find ((== csTopicName) . L.topic) lots of
        Nothing -> []
        Just lot -> catMaybes
          [ x
          | x <- fmap (fromErrorCode . L.errorCode) (L.partitions lot)
          , x /= Just None
          , x /= Nothing
          ]
  case errs of
    [] -> pure (listOffsetsMap listedOffs)
    (err:_) -> throwError (KafkaProtocolException err)

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
toOffsetList = fmap (\(k, v) -> PartitionOffset (fromIntegral k) v) . IM.toList

listOffsetsMap :: ListOffsetsResponse -> IntMap Int64
listOffsetsMap lor = IM.fromList $ fmap
  (\pr -> (fromIntegral (L.partition pr), L.offset pr))
  (concatMap L.partitions $ L.topics lor)

updateOffsets :: TopicName -> IntMap Int64 -> FetchResponse -> IntMap Int64
updateOffsets name current r =
  IM.mapWithKey
    (\pid offs -> fromMaybe offs
      (F.partitionLastSeenOffset r name (fromIntegral pid)))
    current

withConsumer :: ()
  => Peer
  -> ConsumerSettings
  -> (TVar ConsumerState -> IO a)
  -> IO (Either KafkaException a)
withConsumer peer settings f = do
  r <- withKafka peer $ \k -> do
    r <- newConsumer k settings
    case r of
      Left e -> pure (Left e)
      Right state -> fmap Right (f state)
  case r of
    Left e -> pure (Left e)
    Right e -> pure e

newConsumer ::
     Kafka
  -> ConsumerSettings
  -> IO (Either KafkaException (TVar ConsumerState))
newConsumer kafka settings@(ConsumerSettings {..}) = runExceptT $ do
  let initialMember = GroupMember groupName Nothing
  partitionCount <- ExceptT $ getPartitionCount kafka csTopicName timeout handle
  (genId', me, members) <- join kafka csTopicName initialMember handle
  (newGenId, assigns) <- sync kafka csTopicName partitionCount me members genId' handle
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
            , sock = so
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
      sendHeartbeat
      heartbeats

-- | rejoin is called when the client receives a "rebalance in progress"
-- error code, triggered by another client joining or leaving the group.
-- It sends join and sync requests and receives a new member id, generation
-- id, and set of assigned topics.
rejoin :: Consumer ()
rejoin = do
  ConsumerState {kafka, member, partitionCount, settings} <- getv
  let topicName = csTopicName settings
  (genId', newMember, members') <- Consumer $ lift $ join kafka topicName member (handle settings)
  (newGenId, assigns) <- Consumer $ lift $ sync kafka topicName partitionCount newMember members' genId' (handle settings)
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
partitionsForTopic t assigns =
  S.partitions <$> find (\a -> S.topic a == t) assigns

sendHeartbeat :: Consumer ()
sendHeartbeat = do
  ConsumerState {sock, member, genId, kafka, settings} <- getv
  withSocket sock $ do
    liftConsumer $ heartbeat kafka (HeartbeatRequest member genId) (handle settings)
    timeout <- liftIO $ registerDelay (timeout settings)
    resp <- liftConsumer $ tryParse <$> getHeartbeatResponse kafka timeout (handle settings)
    case fromErrorCode (H.errorCode resp) of
      Nothing -> pure ()
      Just None -> pure ()
      Just RebalanceInProgress -> rejoin
      Just x -> throwError (KafkaProtocolException x)

leave :: Consumer ()
leave = do
  ConsumerState {..} <- getv
  withSocket sock $ do
    liftConsumer $ leaveGroup kafka (LeaveGroupRequest member) (handle settings)
    timeout <- liftIO $ registerDelay (timeout settings)
    resp <- liftConsumer $ tryParse <$> getLeaveGroupResponse kafka timeout (handle settings)
    case fromErrorCode (LeaveGroup.errorCode resp) of
      Nothing -> modifyv (\s -> s { quit = Interrupted })
      Just None -> modifyv (\s -> s { quit = Interrupted })
      Just x -> do
        modifyv (\s -> s { quit = Interrupted })
        throwError (KafkaProtocolException x)

getRecordSet :: Int -> Consumer FetchResponse
getRecordSet fetchWaitTime = do
  getsv sock >>= \so -> withSocket so $ do
    ConsumerState {..} <- getv
    let offsetList = toOffsetList offsets
        ConsumerSettings {..} = settings
    liftConsumer $ fetch
      kafka
      (FetchRequest csTopicName fetchWaitTime offsetList maxFetchBytes)
      handle
    interrupt <- liftIO $ registerDelay timeout
    fetchResp <- liftConsumer $ tryParse <$> getFetchResponse kafka interrupt handle
    let errs = fetchResponseErrors fetchResp
    let newOffsets = updateOffsets csTopicName offsets fetchResp
    modifyv (\s -> s { offsets = newOffsets })
    cs <- getv
    when (autoCommit == AutoCommit) (commitOffsets' cs)
    forM_ errs $ \(FetchErrorMessage _ partition errCode) ->
      case fromErrorCode errCode of
        Just None -> pure ()
        Just OffsetOutOfRange -> do
          jumpToLatestOffset partition
        _ -> throwError (KafkaFetchException errs)
    pure fetchResp

jumpToLatestOffset :: Int32 -> Consumer ()
jumpToLatestOffset index = do
  ConsumerState {..} <- getv
  let ConsumerSettings {..} = settings
  liftConsumer $ listOffsets kafka (ListOffsetsRequest csTopicName [index] Latest) handle
  listOffsetsTimeout <- liftIO (registerDelay timeout)
  resp <- liftConsumer $ tryParse <$>
    L.getListOffsetsResponse kafka listOffsetsTimeout handle
  let listedOffs = listOffsetsMap resp
  -- Right biased union, because we want to jump to the offset in the right map
  modifyv (\s -> s { offsets = IM.unionWith (\_ y -> y) offsets listedOffs })
  getv >>= commitOffsets'

{-
_offsetCommitErrors :: C.OffsetCommitResponse -> [OffsetCommitErrorMessage]
_offsetCommitErrors resp =
  let tops = C.topics resp
  in  foldMap
        (\t ->
          [OffsetCommitErrorMessage (C.topic t) p e
            | p <- fmap C.partitionIndex (C.partitions t)
            , e <- fmap C.errorCode (C.partitions t)
            , maybe False (`elem` fatalErrors) (fromErrorCode e)
          ])
        tops
  where
  fatalErrors =
    [ OffsetMetadataTooLarge
    , IllegalGeneration
    , UnknownMemberId
    , InvalidCommitOffsetSize
    ]
-}

fetchResponseErrors :: FetchResponse -> [FetchErrorMessage]
fetchResponseErrors resp =
  let tops = topics resp
  in  flip foldMap tops $ \t ->
        [ FetchErrorMessage (topic t) p e
        | p <- fmap (partition . partitionHeader) (partitions t)
        , e <- fmap (partitionHeaderErrorCode . partitionHeader) (partitions t)
        , maybe False (`elem` fatalErrors) (fromErrorCode e)
        ]
  where
  fatalErrors =
    [ UnknownServerError
    , OffsetOutOfRange
    , UnknownTopicOrPartition
    , NotLeaderForPartition
    , ReplicaNotAvailable
    ]

latestOffsets :: [Int32] -> Consumer (IntMap Int64)
latestOffsets indices = do
  ConsumerState {..} <- getv
  liftConsumer $ offsetFetch
    kafka
    (OffsetFetchRequest (csTopicName settings) member indices)
    (handle settings)
  timeout <- liftIO $ registerDelay (timeout settings)
  offs <- liftConsumer $ tryParse <$> O.getOffsetFetchResponse kafka timeout (handle settings)
  case fromErrorCode (O.errorCode offs) of
    Nothing -> pure (offsetFetchOffsets offs)
    Just None -> pure (offsetFetchOffsets offs)
    Just x -> throwError (KafkaProtocolException x)

offsetFetchOffsets :: O.OffsetFetchResponse -> IntMap Int64
offsetFetchOffsets ofr = IM.fromList $ fmap
  (\part -> (fromIntegral $ O.partitionIndex part, O.offset part))
  (concatMap O.partitions $ O.topics ofr)

commitOffsets' :: ConsumerState -> Consumer ()
commitOffsets' cs = do
  let ConsumerState {..} = cs
  let ConsumerSettings {..} = settings
  liftConsumer $ offsetCommit
    kafka
    (OffsetCommitRequest csTopicName (toOffsetList offsets) member genId)
    handle
  timeoutV <- liftIO $ registerDelay timeout
  resp <- liftConsumer $ tryParse <$> C.getOffsetCommitResponse kafka timeoutV handle
  let tops = C.topics resp
  let errs = case List.find ((== csTopicName) . C.topic) tops of
        Nothing -> []
        Just top -> catMaybes
          [ x
          | x <- fmap (fromErrorCode . C.errorCode) (C.partitions top)
          , x /= Just None
          , x /= Nothing
          ]
  case errs of
    [] -> pure ()
    (err:_) -> throwError (KafkaProtocolException err)

  --let errs = offsetCommitErrors resp
  --when (not (null errs)) $ do
  --  throwError (KafkaOffsetCommitException errs)

commitOffsets :: Consumer ()
commitOffsets = do
  cs@ConsumerState {..} <- getv
  withSocket sock (commitOffsets' cs)

assignMembers :: Int -> TopicName -> Int32 -> [Member] -> [MemberAssignment]
assignMembers memberCount topicName partitionCount groupMembers =
  fmap (assignMember memberCount topicName partitionCount) (List.zip groupMembers [0..])

assignMember :: Int -> TopicName -> Int32 -> (Member, Int) -> MemberAssignment
assignMember memberCount topicName partitionCount (member, i) =
  MemberAssignment
    memberName
    [TopicAssignment (coerce topicName) assignments]
  where
  memberName = J.groupMemberId member
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

-- We want to wait a (potentially) long time for a response
-- when we join the group because the server may be waiting
-- on members from a previous generation who didn't leave properly
joinTimeout :: Int
joinTimeout = 30000000

sync ::
     Kafka
  -> TopicName
  -> Int32
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> Maybe Handle
  -> ExceptT KafkaException IO (GenerationId, [SyncTopicAssignment])
sync kafka topicName partitionCount member members genId handle = do
  let assignments = assignMembers (length members) topicName partitionCount members
  ExceptT $ syncGroup
    kafka
    (SyncGroupRequest member genId assignments)
    handle
  wait <- liftIO (registerDelay joinTimeout)
  sgr <- ExceptT $ tryParse <$> S.getSyncGroupResponse kafka wait handle
  if S.errorCode sgr `elem` expectedSyncErrors then do
    (newGenId, newMember, newMembers) <- join kafka topicName member handle
    sync kafka topicName partitionCount newMember newMembers newGenId handle
  else if S.errorCode sgr == noError then do
    let assigns = S.partitionAssignments <$> S.memberAssignment sgr
    pure (genId, fromMaybe [] assigns)
  else
    throwError (KafkaUnexpectedErrorCodeException (S.errorCode sgr))

join ::
     Kafka
  -> TopicName
  -> GroupMember
  -> Maybe Handle
  -> ExceptT KafkaException IO (GenerationId, GroupMember, [Member])
join kafka top member@(GroupMember name@(GroupName gid) _) handle = do
  ExceptT $ findCoordinator
    kafka
    (FindCoordinatorRequest (Str.toByteArray gid) 0)
    handle
  wait <- liftIO (registerDelay joinTimeout)
  -- Ignoring the response from FindCoordinator. Probably not
  -- a good idea.
  _ <- ExceptT $ tryParse <$> getFindCoordinatorResponse kafka wait handle
  ExceptT $ joinGroup
    kafka
    (JoinGroupRequest top member)
    handle
  jgr <- ExceptT $ tryParse <$> getJoinGroupResponse kafka wait handle
  let no_error_join = do
        let genId = GenerationId (J.generationId jgr)
        let memId = Just (J.memberId jgr)
        let assignment = GroupMember name memId
        pure (genId, assignment, J.members jgr)
  case fromErrorCode (J.errorCode jgr) of
    Nothing -> no_error_join
    Just None -> no_error_join
    Just MemberIdRequired -> do
      let memId = Just (J.memberId jgr)
      let assignment = GroupMember name memId
      join kafka top assignment handle
    Just x -> throwError (KafkaProtocolException x)

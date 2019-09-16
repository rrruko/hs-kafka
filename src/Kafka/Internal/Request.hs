{-# LANGUAGE RecordWildCards #-}

module Kafka.Internal.Request
  ( fetch
  , heartbeat
  , joinGroup
  , findCoordinator
  , leaveGroup
  , listOffsets
  , metadata
  , offsetCommit
  , offsetFetch
  , produce
  , syncGroup
  ) where

import Data.Bifunctor (first)
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Socket.Stream.Uninterruptible.Bytes
import System.IO (Handle, hPutStrLn)

import Kafka.Common
import Kafka.Internal.Fetch.Request
import Kafka.Internal.FindCoordinator.Request
import Kafka.Internal.Heartbeat.Request
import Kafka.Internal.JoinGroup.Request
import Kafka.Internal.LeaveGroup.Request
import Kafka.Internal.ListOffsets.Request
import Kafka.Internal.Metadata.Request
import Kafka.Internal.OffsetCommit.Request
import Kafka.Internal.OffsetFetch.Request
import Kafka.Internal.Produce.Request
import Kafka.Internal.Request.Types
import Kafka.Internal.ShowDebug
import Kafka.Internal.SyncGroup.Request

request ::
     Kafka
  -> UnliftedArray ByteArray
  -> IO (Either KafkaException ())
request kafka msg = first KafkaSendException <$> sendMany (getKafka kafka) msg

logHandle :: Maybe Handle -> String -> IO ()
logHandle handle str =
  case handle of
    Nothing -> pure ()
    Just h -> hPutStrLn h str

produce ::
     Kafka
  -> ProduceRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
produce kafka req@ProduceRequest{..} handle = do
  logHandle handle (showDebug req)
  let Topic topicName parts ctr = produceTopic
  p <- fromIntegral <$> readIORef ctr
  let message = produceRequest
        (produceWaitTime `div` 1000)
        (TopicName topicName)
        p
        producePayloads
  e <- request kafka message
  either (pure . Left) (\a -> increment parts ctr >> pure (Right a)) e

--
-- [Note: Partitioning algorithm]
-- We increment the partition pointer after each batch
-- production. We currently do not handle overflow of partitions
-- (i.e. we don't pay attention to max_bytes)
increment :: ()
  => Int -- ^ number of partitions
  -> IORef Int -- ^ incrementing number
  -> IO ()
increment totalParts ref = modifyIORef' ref $ \ptr ->
  -- 0-indexed vs 1-indexed
  -- (ptr vs total number of partitions)
  if ptr + 1 == totalParts
    then 0
    else ptr + 1

fetch ::
     Kafka
  -> FetchRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
fetch kafka req@FetchRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ sessionlessFetchRequest
    (fetchWaitTime `div` 1000)
    fetchTopic
    fetchPartitionOffsets
    fetchMaxBytes

listOffsets ::
     Kafka
  -> ListOffsetsRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
listOffsets kafka req@ListOffsetsRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ listOffsetsRequest
    listOffsetsTopic
    listOffsetsIndices
    listOffsetsTimestamp

joinGroup ::
     Kafka
  -> JoinGroupRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
joinGroup kafka req@JoinGroupRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ joinGroupRequest
    joinGroupTopic
    joinGroupMember

-- Like most of the other functions in this module, findCoordinator
-- largely ignores the response from kafka. This is probably not a
-- good idea, and it may need to be revisited eventually.
findCoordinator ::
     Kafka
  -> FindCoordinatorRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
findCoordinator kafka req@FindCoordinatorRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ findCoordinatorRequest
    findCoordinatorKey
    findCoordinatorKeyType

heartbeat ::
     Kafka
  -> HeartbeatRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
heartbeat kafka req@HeartbeatRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ heartbeatRequest
    heartbeatMember
    heartbeatGenId

syncGroup ::
     Kafka
  -> SyncGroupRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
syncGroup kafka req@SyncGroupRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ syncGroupRequest
    syncGroupMember
    syncGroupGenId
    syncGroupAssignments

offsetCommit ::
     Kafka
  -> OffsetCommitRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
offsetCommit kafka req@OffsetCommitRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ offsetCommitRequest
    offsetCommitTopic
    offsetCommitOffsets
    offsetCommitMember
    offsetCommitGenId

offsetFetch ::
     Kafka
  -> OffsetFetchRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
offsetFetch kafka req@OffsetFetchRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ offsetFetchRequest
    offsetFetchMember
    offsetFetchTopic
    offsetFetchIndices

leaveGroup ::
     Kafka
  -> LeaveGroupRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
leaveGroup kafka req@LeaveGroupRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ leaveGroupRequest
    leaveGroupMember

metadata ::
     Kafka
  -> MetadataRequest
  -> Maybe Handle
  -> IO (Either KafkaException ())
metadata kafka req@MetadataRequest{..} handle = do
  logHandle handle (showDebug req)
  request kafka $ metadataRequest
    metadataTopic
    metadataAutoCreateTopic

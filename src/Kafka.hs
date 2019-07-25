module Kafka
  ( fetch
  , heartbeat
  , joinGroup
  , leaveGroup
  , listOffsets
  , offsetCommit
  , offsetFetch
  , produce
  , syncGroup
  ) where

import Data.Bifunctor (first)
import Data.Int
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Socket.Stream.Uninterruptible.Bytes

import Kafka.Common
import Kafka.Fetch.Request
import Kafka.Heartbeat.Request
import Kafka.JoinGroup.Request
import Kafka.LeaveGroup.Request
import Kafka.ListOffsets.Request
import Kafka.OffsetCommit.Request
import Kafka.OffsetFetch.Request
import Kafka.Produce.Request
import Kafka.SyncGroup.Request

request ::
     Kafka
  -> UnliftedArray ByteArray
  -> IO (Either KafkaException ())
request kafka msg = first KafkaSendException <$> sendMany (getKafka kafka) msg

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException ())
produce kafka (Topic topicName parts ctr) waitTime payloads = do
  p <- fromIntegral <$> readIORef ctr
  let message = produceRequest
        (waitTime `div` 1000)
        (TopicName topicName)
        p
        payloads
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
  -> TopicName
  -> Int
  -> [PartitionOffset]
  -> IO (Either KafkaException ())
fetch kafka topic waitTime partitions =
  request kafka $ sessionlessFetchRequest (waitTime `div` 1000) topic partitions

listOffsets ::
     Kafka
  -> TopicName
  -> [Int32]
  -> IO (Either KafkaException ())
listOffsets kafka topic partitionIndices =
  request kafka $ listOffsetsRequest topic partitionIndices

joinGroup ::
     Kafka
  -> TopicName
  -> GroupMember
  -> IO (Either KafkaException ())
joinGroup kafka topic groupMember =
  request kafka $ joinGroupRequest topic groupMember

heartbeat ::
     Kafka
  -> GroupMember
  -> GenerationId
  -> IO (Either KafkaException ())
heartbeat kafka groupMember generationId =
  request kafka $ heartbeatRequest groupMember generationId

syncGroup ::
     Kafka
  -> GroupMember
  -> GenerationId
  -> [MemberAssignment]
  -> IO (Either KafkaException ())
syncGroup kafka groupMember generationId assignments =
  request kafka $ syncGroupRequest groupMember generationId assignments

offsetCommit ::
     Kafka
  -> TopicName
  -> [PartitionOffset]
  -> GroupMember
  -> GenerationId
  -> IO (Either KafkaException ())
offsetCommit kafka top offs groupMember generationId =
  request kafka $ offsetCommitRequest top offs groupMember generationId

offsetFetch ::
     Kafka
  -> GroupMember
  -> TopicName
  -> [Int32]
  -> IO (Either KafkaException ())
offsetFetch kafka groupMember top offs =
  request kafka $ offsetFetchRequest groupMember top offs

leaveGroup ::
     Kafka
  -> GroupMember
  -> IO (Either KafkaException ())
leaveGroup kafka groupMember =
  request kafka $ leaveGroupRequest groupMember

module Kafka.Internal.OffsetCommit.Response
  ( OffsetCommitPartition(..)
  , OffsetCommitResponse(..)
  , OffsetCommitTopic(..)
  , getOffsetCommitResponse
  , parseOffsetCommitResponse
  ) where

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data OffsetCommitResponse = OffsetCommitResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , topics :: [OffsetCommitTopic]
  } deriving (Eq, Show)

data OffsetCommitTopic = OffsetCommitTopic
  { topic :: {-# UNPACK #-} !TopicName
  , partitions :: [OffsetCommitPartition]
  } deriving (Eq, Show)

data OffsetCommitPartition = OffsetCommitPartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseOffsetCommitResponse :: Parser OffsetCommitResponse
parseOffsetCommitResponse = do
  _correlationId <- int32 "correlation id"
  OffsetCommitResponse
    <$> (int32 "throttle time")
    <*> (array parseOffsetCommitTopic <?> "topics")

parseOffsetCommitTopic :: Parser OffsetCommitTopic
parseOffsetCommitTopic = OffsetCommitTopic
  <$> (topicName <?> "topic name")
  <*> (array parseOffsetCommitPartitions <?> "partitions")

parseOffsetCommitPartitions :: Parser OffsetCommitPartition
parseOffsetCommitPartitions = OffsetCommitPartition
  <$> (int32 "partition id")
  <*> (int16 "error code")

getOffsetCommitResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String OffsetCommitResponse))
getOffsetCommitResponse = fromKafkaResponse parseOffsetCommitResponse

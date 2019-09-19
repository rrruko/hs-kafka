{-# language
    BangPatterns
  #-}

module Kafka.Internal.OffsetFetch.Response
  ( OffsetFetchResponse(..)
  , OffsetFetchTopic(..)
  , OffsetFetchPartition(..)
  , getOffsetFetchResponse
  , parseOffsetFetchResponse
  ) where

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data OffsetFetchResponse = OffsetFetchResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , topics :: [OffsetFetchTopic]
  , errorCode :: !Int16
  } deriving (Eq, Show)

data OffsetFetchTopic = OffsetFetchTopic
  { topic :: {-# UNPACK #-} !TopicName
  , partitions :: [OffsetFetchPartition]
  } deriving (Eq, Show)

data OffsetFetchPartition = OffsetFetchPartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , offset :: {-# UNPACK #-} !Int64
  , leaderEpoch :: {-# UNPACK #-} !Int32
  , metadata :: !(Maybe ByteArray)
  , partitionErrorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseOffsetFetchResponse :: Parser OffsetFetchResponse
parseOffsetFetchResponse = do
  _correlationId <- int32 "correlation id"
  OffsetFetchResponse
    <$> (int32 "throttle time")
    <*> (array parseOffsetFetchTopic) -- topics <?> "topics")
    <*> (int16 "error code")

parseOffsetFetchTopic :: Parser OffsetFetchTopic
parseOffsetFetchTopic = OffsetFetchTopic
  <$> (topicName) --String <?> "topic name")
  <*> (array parseOffsetFetchPartitions) -- <?> "partitions")

parseOffsetFetchPartitions :: Parser OffsetFetchPartition
parseOffsetFetchPartitions = OffsetFetchPartition
  <$> (int32 "partition id")
  <*> (int64 "offset")
  <*> (int32 "leader epoch")
  <*> nullableByteArray -- <?> "metadata")
  <*> (int16 "error code")

getOffsetFetchResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String OffsetFetchResponse))
getOffsetFetchResponse = fromKafkaResponse parseOffsetFetchResponse

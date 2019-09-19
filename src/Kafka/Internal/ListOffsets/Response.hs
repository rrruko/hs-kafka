{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.ListOffsets.Response
  ( ListOffsetsResponse(..)
  , ListOffsetsTopic(..)
  , ListOffsetPartition(..)
  , getListOffsetsResponse
  ) where

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data ListOffsetsResponse = ListOffsetsResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , topics :: [ListOffsetsTopic]
  } deriving (Eq, Show)

data ListOffsetsTopic = ListOffsetsTopic
  { topic :: {-# UNPACK #-} !TopicName
  , partitions :: [ListOffsetPartition]
  } deriving (Eq, Show)

data ListOffsetPartition = ListOffsetPartition
  { partition :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , timestamp :: {-# UNPACK #-} !Int64
  , offset :: {-# UNPACK #-} !Int64
  , leaderEpoch :: {-# UNPACK #-} !Int32
  } deriving (Eq, Show)

parseListOffsetsResponse :: Parser ListOffsetsResponse
parseListOffsetsResponse = do
  _correlationId <- int32 "correlation id"
  ListOffsetsResponse
    <$> int32 "throttleTimeMs"
    <*> array parseListOffsetsTopic

parseListOffsetsTopic :: Parser ListOffsetsTopic
parseListOffsetsTopic = ListOffsetsTopic
  <$> topicName
  <*> array parseListOffsetPartition

parseListOffsetPartition :: Parser ListOffsetPartition
parseListOffsetPartition = ListOffsetPartition
  <$> int32 "partition"
  <*> int16 "error code"
  <*> int64 "timestamp"
  <*> int64 "offset"
  <*> int32 "leader epoch"

getListOffsetsResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String ListOffsetsResponse))
getListOffsetsResponse = fromKafkaResponse parseListOffsetsResponse

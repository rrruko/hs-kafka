module Kafka.OffsetCommit.Response
  ( OffsetCommitPartition(..)
  , OffsetCommitResponse(..)
  , OffsetCommitTopic(..)
  , getOffsetCommitResponse
  , parseOffsetCommitResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data OffsetCommitResponse = OffsetCommitResponse
  { throttleTimeMs :: Int32
  , offsetCommitTopics :: [OffsetCommitTopic]
  } deriving (Eq, Show)

data OffsetCommitTopic = OffsetCommitTopic
  { offsetCommitTopicName :: ByteString
  , offsetCommitPartitions :: [OffsetCommitPartition]
  } deriving (Eq, Show)

data OffsetCommitPartition = OffsetCommitPartition
  { offsetCommitPartitionIndex :: Int32
  , offsetCommitErrorCode :: Int16
  } deriving (Eq, Show)

parseOffsetCommitResponse :: Parser OffsetCommitResponse
parseOffsetCommitResponse = do
  _correlationId <- int32 <?> "correlation id"
  OffsetCommitResponse
    <$> (int32 <?> "throttle time")
    <*> (array parseOffsetCommitTopic <?> "topics")

parseOffsetCommitTopic :: Parser OffsetCommitTopic
parseOffsetCommitTopic = do
  OffsetCommitTopic
    <$> (byteString <?> "topic name")
    <*> (array parseOffsetCommitPartitions <?> "partitions")

parseOffsetCommitPartitions :: Parser OffsetCommitPartition
parseOffsetCommitPartitions = do
  OffsetCommitPartition
    <$> (int32 <?> "partition id")
    <*> (int16 <?> "error code")

getOffsetCommitResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String OffsetCommitResponse))
getOffsetCommitResponse = fromKafkaResponse parseOffsetCommitResponse

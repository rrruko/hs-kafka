module Kafka.Internal.OffsetCommit.Response
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
import System.IO

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data OffsetCommitResponse = OffsetCommitResponse
  { throttleTimeMs :: Int32
  , topics :: [OffsetCommitTopic]
  } deriving (Eq, Show)

data OffsetCommitTopic = OffsetCommitTopic
  { topic :: ByteString
  , partitions :: [OffsetCommitPartition]
  } deriving (Eq, Show)

data OffsetCommitPartition = OffsetCommitPartition
  { partitionIndex :: Int32
  , errorCode :: Int16
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
  -> Maybe Handle
  -> IO (Either KafkaException (Either String OffsetCommitResponse))
getOffsetCommitResponse = fromKafkaResponse parseOffsetCommitResponse

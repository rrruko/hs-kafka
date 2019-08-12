module Kafka.OffsetFetch.Response
  ( OffsetFetchResponse(..)
  , OffsetFetchTopic(..)
  , OffsetFetchPartition(..)
  , getOffsetFetchResponse
  , parseOffsetFetchResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data OffsetFetchResponse = OffsetFetchResponse
  { throttleTimeMs :: Int32
  , topics :: [OffsetFetchTopic]
  , errorCode :: Int16
  } deriving (Eq, Show)

data OffsetFetchTopic = OffsetFetchTopic
  { offsetFetchTopic :: ByteString
  , offsetFetchPartitions :: [OffsetFetchPartition]
  } deriving (Eq, Show)

data OffsetFetchPartition = OffsetFetchPartition
  { offsetFetchPartitionIndex :: Int32
  , offsetFetchOffset :: Int64
  , offsetFetchLeaderEpoch :: Int32
  , offsetFetchMetadata :: Maybe ByteString
  , offsetFetchErrorCode :: Int16
  } deriving (Eq, Show)

parseOffsetFetchResponse :: Parser OffsetFetchResponse
parseOffsetFetchResponse = do
  _correlationId <- int32 <?> "correlation id"
  OffsetFetchResponse
    <$> (int32 <?> "throttle time")
    <*> (array parseOffsetFetchTopic <?> "topics")
    <*> (int16 <?> "error code")

parseOffsetFetchTopic :: Parser OffsetFetchTopic
parseOffsetFetchTopic = do
  OffsetFetchTopic
    <$> (byteString <?> "topic name")
    <*> (array parseOffsetFetchPartitions <?> "partitions")

parseOffsetFetchPartitions :: Parser OffsetFetchPartition
parseOffsetFetchPartitions = do
  OffsetFetchPartition
    <$> (int32 <?> "partition id")
    <*> (int64 <?> "offset")
    <*> (int32 <?> "leader epoch")
    <*> (nullableByteString <?> "metadata")
    <*> (int16 <?> "error code")

getOffsetFetchResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String OffsetFetchResponse))
getOffsetFetchResponse = fromKafkaResponse parseOffsetFetchResponse

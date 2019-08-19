module Kafka.Metadata.Response
  ( MetadataResponse(..)
  , MetadataBroker(..)
  , MetadataTopic(..)
  , MetadataPartition(..)
  , getMetadataResponse
  , parseMetadataResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data MetadataResponse = MetadataResponse
  { throttleTimeMs :: Int32
  , brokers :: [MetadataBroker]
  , clusterId :: Maybe ByteString
  , controllerId :: Int32
  , topics :: [MetadataTopic]
  } deriving (Eq, Show)

data MetadataBroker = MetadataBroker
  { nodeId :: Int32
  , host :: ByteString
  , port :: Int32
  , rack :: Maybe ByteString
  } deriving (Eq, Show)

data MetadataTopic = MetadataTopic
  { errorCode :: Int16
  , name :: ByteString
  , isInternal :: Bool
  , partitions :: [MetadataPartition]
  } deriving (Eq, Show)

data MetadataPartition = MetadataPartition
  { partitionErrorCode :: Int16
  , partitionIndex :: Int32
  , leaderId :: Int32
  , leaderEpoch :: Int32
  , replicaNodes :: Int32
  , isrNodes :: Int32
  , offlineReplicas :: Int32
  } deriving (Eq, Show)

parseMetadataResponse :: Parser MetadataResponse
parseMetadataResponse = do
  _correlationId <- int32 <?> "correlation id"
  MetadataResponse
    <$> (int32 <?> "throttle time")
    <*> (array parseMetadataBroker <?> "brokers")
    <*> (nullableByteString <?> "cluster id")
    <*> (int32 <?> "controller id")
    <*> (array parseMetadataTopic <?> "topics")

parseMetadataBroker :: Parser MetadataBroker
parseMetadataBroker = do
  MetadataBroker
    <$> (int32 <?> "node id")
    <*> (byteString <?> "host")
    <*> (int32 <?> "port")
    <*> (nullableByteString <?> "rack")

parseMetadataTopic :: Parser MetadataTopic
parseMetadataTopic = do
  MetadataTopic
    <$> (int16 <?> "error code")
    <*> (byteString <?> "name")
    <*> (bool <?> "is internal")
    <*> (array parseMetadataPartition <?> "partitions")

parseMetadataPartition :: Parser MetadataPartition
parseMetadataPartition = do
  MetadataPartition
    <$> (int16 <?> "error code")
    <*> (int32 <?> "partition index")
    <*> (int32 <?> "leader id")
    <*> (int32 <?> "leader epoch")
    <*> (int32 <?> "replica nodes")
    <*> (int32 <?> "isr nodes")
    <*> (int32 <?> "offline replicas")

getMetadataResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String MetadataResponse))
getMetadataResponse = fromKafkaResponse parseMetadataResponse

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
  { metadataThrottle :: Int32
  , metadataBrokers :: [MetadataBroker]
  , metadataClusterId :: Maybe ByteString
  , metadataControllerId :: Int32
  , metadataTopics :: [MetadataTopic]
  } deriving (Eq, Show)

data MetadataBroker = MetadataBroker
  { metadataNodeId :: Int32
  , metadataHost :: ByteString
  , metadataPort :: Int32
  , metadataRack :: Maybe ByteString
  } deriving (Eq, Show)

data MetadataTopic = MetadataTopic
  { mtErrorCode :: Int16
  , mtName :: ByteString
  , mtIsInternal :: Bool
  , mtPartitions :: [MetadataPartition]
  } deriving (Eq, Show)

data MetadataPartition = MetadataPartition
  { mpErrorCode :: Int16
  , mpPartitionIndex :: Int32
  , mpLeaderId :: Int32
  , mpLeaderEpoch :: Int32
  , mpReplicaNodes :: Int32
  , mpIsrNodes :: Int32
  , mpOfflineReplicas :: Int32
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

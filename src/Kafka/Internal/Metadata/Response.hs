module Kafka.Internal.Metadata.Response
  ( MetadataResponse(..)
  , MetadataBroker(..)
  , MetadataTopic(..)
  , MetadataPartition(..)
  , getMetadataResponse
  , parseMetadataResponse
  ) where

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data MetadataResponse = MetadataResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , brokers :: [MetadataBroker]
  , clusterId :: !(Maybe ByteArray)
  , controllerId :: {-# UNPACK #-} !Int32
  , topics :: [MetadataTopic]
  } deriving (Eq, Show)

data MetadataBroker = MetadataBroker
  { nodeId :: {-# UNPACK #-} !Int32
  , host :: {-# UNPACK #-} !ByteArray
  , port :: {-# UNPACK #-} !Int32
  , rack :: !(Maybe ByteArray)
  } deriving (Eq, Show)

data MetadataTopic = MetadataTopic
  { errorCode :: {-# UNPACK #-} !Int16
  , name :: !TopicName
  , isInternal :: !Bool
  , partitions :: [MetadataPartition]
  } deriving (Eq, Show)

data MetadataPartition = MetadataPartition
  { partitionErrorCode :: {-# UNPACK #-} !Int16
  , partitionIndex :: {-# UNPACK #-} !Int32
  , leaderId :: {-# UNPACK #-} !Int32
  , leaderEpoch :: {-# UNPACK #-} !Int32
  , replicaNodes :: {-# UNPACK #-} !Int32
  , isrNodes :: {-# UNPACK #-} !Int32
  , offlineReplicas :: {-# UNPACK #-} !Int32
  } deriving (Eq, Show)

parseMetadataResponse :: Parser MetadataResponse
parseMetadataResponse = do
  _correlationId <- int32 "correlation id"
  MetadataResponse
    <$> (int32 "throttle time")
    <*> (array parseMetadataBroker <?> "brokers")
    <*> (nullableByteArray <?> "cluster id")
    <*> (int32 "controller id")
    <*> (array parseMetadataTopic <?> "topics")

parseMetadataBroker :: Parser MetadataBroker
parseMetadataBroker = MetadataBroker
  <$> (int32 "node id")
  <*> (bytearray <?> "host")
  <*> (int32 "port")
  <*> (nullableByteArray <?> "rack")

parseMetadataTopic :: Parser MetadataTopic
parseMetadataTopic = do
  MetadataTopic
    <$> (int16 "error code")
    <*> (topicName <?> "name")
    <*> (bool "is internal")
    <*> (array parseMetadataPartition <?> "partitions")

parseMetadataPartition :: Parser MetadataPartition
parseMetadataPartition = do
  MetadataPartition
    <$> (int16 "error code")
    <*> (int32 "partition index")
    <*> (int32 "leader id")
    <*> (int32 "leader epoch")
    <*> (int32 "replica nodes")
    <*> (int32 "isr nodes")
    <*> (int32 "offline replicas")

getMetadataResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String MetadataResponse))
getMetadataResponse = fromKafkaResponse parseMetadataResponse

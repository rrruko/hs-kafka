{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}

module Kafka.Common where

import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.Bytes.Types
import Data.Coerce
import Data.Int
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Data.Text
import Data.Word
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4

import qualified Data.ByteString as BS

newtype Kafka = Kafka { getKafka :: Connection }

instance Show Kafka where
  show _ = "<Kafka>"

data Topic = Topic
  ByteArray -- Topic name
  Int -- Number of partitions
  (IORef Int) -- incrementing number

newtype TopicName = TopicName ByteArray
  deriving (Eq, Show)

getTopicName :: Topic -> TopicName
getTopicName (Topic name _ _) = TopicName name

data PartitionOffset = PartitionOffset
  { partitionIndex :: Int32
  , partitionOffset :: Int64
  } deriving (Eq, Show)

data KafkaTimestamp
  = Latest
  | Earliest
  | At Int64
  deriving (Show)

data AutoCreateTopic
  = Create
  | NeverCreate
  deriving (Show)

data KafkaException where
  KafkaSendException :: SendException 'Uninterruptible -> KafkaException
  KafkaReceiveException :: ReceiveException 'Interruptible -> KafkaException
  KafkaParseException :: String -> KafkaException
  KafkaUnexpectedErrorCodeException :: Int16 -> KafkaException
  KafkaConnectException :: ConnectException ('Internet 'V4) 'Uninterruptible -> KafkaException
  KafkaException :: Text -> KafkaException

deriving stock instance Show KafkaException

data GroupMember = GroupMember ByteArray (Maybe ByteArray)
  deriving (Eq, Show)

data GenerationId = GenerationId
  { getGenerationId :: Int32
  } deriving (Eq, Show)

data MemberAssignment = MemberAssignment
  { assignedMemberId :: ByteArray
  , assignedTopics :: [TopicAssignment]
  } deriving (Eq, Show)

data TopicAssignment = TopicAssignment
  { assignedTopicName :: ByteArray
  , assignedPartitions :: [Int32]
  } deriving (Eq, Show)

data Interruptedness = Interrupted | Uninterrupted
  deriving (Eq, Show)

newKafka :: Peer -> IO (Either KafkaException Kafka)
newKafka = fmap (first KafkaConnectException) . coerce . connect

defaultKafka :: Peer
defaultKafka = Peer (IPv4 0) 9092

toBE16 :: Int16 -> Int16
toBE16 = fromIntegral . byteSwap16 . fromIntegral

toBE32 :: Int32 -> Int32
toBE32 = fromIntegral . byteSwap32 . fromIntegral

toBEW32 :: Word32 -> Word32
toBEW32 = byteSwap32

toBE64 :: Int64 -> Int64
toBE64 = fromIntegral . byteSwap64 . fromIntegral

size8 :: ByteArray -> Int8
size8 = fromIntegral . sizeofByteArray

size16 :: ByteArray -> Int16
size16 = fromIntegral . sizeofByteArray

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

getArray :: MutableBytes s -> MutableByteArray s
getArray (MutableBytes a _ _) = a

toByteString :: ByteArray -> ByteString
toByteString = BS.pack . foldrByteArray (:) []

fromByteString :: ByteString -> ByteArray
fromByteString = byteArrayFromList . BS.unpack

foldByteArrays :: UnliftedArray ByteArray -> ByteArray
foldByteArrays = foldrUnliftedArray (<>) (byteArrayFromList ([]::[Char]))

clientId :: ByteString
clientId = "ruko"

clientIdLength :: Int
clientIdLength = BS.length clientId

correlationId :: Int32
correlationId = 0xbeef

magic :: Int8
magic = 2

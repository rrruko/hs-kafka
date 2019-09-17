{-# language
    BangPatterns
  , DataKinds
  , DerivingStrategies
  , GADTs
  , OverloadedStrings
  , ScopedTypeVariables
  , StandaloneDeriving
  , ViewPatterns
  #-}

module Kafka.Common where

import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.Coerce
import Data.Int
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Data.Text
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC8

newtype Kafka = Kafka { getKafka :: Connection }

instance Show Kafka where
  show _ = "<Kafka>"

data Topic = Topic
  ByteArray -- Topic name
  Int -- Number of partitions
  (IORef Int) -- incrementing number

newtype TopicName = TopicName ByteArray
  deriving (Eq, Ord, Show)

getTopicName :: Topic -> TopicName
getTopicName (Topic name _ _) = coerce name

mkTopicName :: String -> TopicName
mkTopicName = coerce . fromString

data PartitionOffset = PartitionOffset
  { partitionIndex :: !Int32
  , partitionOffset :: !Int64
  } deriving (Eq, Show)

data KafkaTimestamp
  = Latest
  | Earliest
  | At !Int64
  deriving (Show)

data AutoCreateTopic
  = Create
  | NeverCreate
  deriving (Show)

data KafkaException where
  KafkaSendException :: ()
    => SendException 'Uninterruptible
    -> KafkaException
  KafkaReceiveException :: ()
    => ReceiveException 'Interruptible
    -> KafkaException
  KafkaCloseException :: ()
    => CloseException
    -> KafkaException
  KafkaParseException :: ()
    => String
    -> KafkaException
  KafkaUnexpectedErrorCodeException :: ()
    => !Int16
    -> KafkaException
  KafkaConnectException :: ()
    => ConnectException ('Internet 'V4) 'Uninterruptible
    -> KafkaException
  KafkaException :: ()
    => !Text
    -> KafkaException
  KafkaOffsetCommitException :: ()
    => [OffsetCommitErrorMessage]
    -> KafkaException
  KafkaFetchException :: ()
    => [FetchErrorMessage]
    -> KafkaException
  KafkaProduceException :: ()
    => !Int16
    -> KafkaException

data OffsetCommitErrorMessage = OffsetCommitErrorMessage
  { commitErrorTopic :: BS.ByteString
  , commitErrorPartition :: Int32
  , commitErrorCode :: Int16
  } deriving Show

data FetchErrorMessage = FetchErrorMessage
  { fetchErrorTopic :: BS.ByteString
  , fetchErrorPartition :: Int32
  , fetchErrorCode :: Int16
  } deriving Show

deriving stock instance Show KafkaException

data GroupMember = GroupMember !ByteArray !(Maybe ByteArray)
  deriving (Eq, Show)

data GenerationId = GenerationId
  { getGenerationId :: !Int32
  } deriving (Eq, Show)

data MemberAssignment = MemberAssignment
  { assignedMemberId :: !ByteArray
  , assignedTopics :: [TopicAssignment]
  } deriving (Eq, Show)

data TopicAssignment = TopicAssignment
  { assignedTopicName :: !ByteArray
  , assignedPartitions :: [Int32]
  } deriving (Eq, Show)

data Interruptedness = Interrupted | Uninterrupted
  deriving (Eq, Show)

withKafka :: ()
  => Peer
  -> (Kafka -> IO a)
  -> IO (Either KafkaException a)
withKafka peer f = do
  r <- withConnection
    peer
    (\e a -> case e of
      Left c -> pure (Left (KafkaCloseException c))
      Right () -> pure a
    )
    (\(Kafka -> conn) -> fmap Right (f conn)
    )
  case r of
    Left e -> pure (Left (KafkaConnectException e))
    Right x -> pure x

-- | Attempt to open a connection to Kafka.
newKafka :: Peer -> IO (Either KafkaException Kafka)
newKafka = fmap (first KafkaConnectException) . coerce . connect

-- | Kafka on localhost.
defaultKafka :: Peer
defaultKafka = Peer (IPv4 0) 9092

toByteString :: ByteArray -> ByteString
toByteString = BS.pack . foldrByteArray (:) []

fromByteString :: ByteString -> ByteArray
fromByteString = byteArrayFromList . BS.unpack

fromString :: String -> ByteArray
fromString = fromByteString . BC8.pack

messages :: [String] -> UnliftedArray ByteArray
messages = unliftedArrayFromList . fmap fromString

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

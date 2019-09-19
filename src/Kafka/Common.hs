{-# language
    BangPatterns
  , DataKinds
  , DerivingStrategies
  , GeneralizedNewtypeDeriving
  , GADTs
  , OverloadedStrings
  , ScopedTypeVariables
  , StandaloneDeriving
  , ViewPatterns
  #-}

module Kafka.Common
  ( Kafka(..)
  , withKafka
  , Topic(..)
  , TopicName(..)
  , getTopicName
  , PartitionOffset(..)
  , TopicAssignment(..)
  , Interruptedness(..)
  , KafkaTimestamp(..)
  , AutoCreateTopic(..)
  , KafkaException(..)
  , GroupMember(..)
  , GenerationId(..)
  , MemberAssignment(..)
  , FetchErrorMessage(..)
  , OffsetCommitErrorMessage(..)
  , clientId
  , clientIdLength
  , correlationId
  , magic
  ) where

--import Data.Primitive.Unlifted.Array
import Data.Text (Text)
import Socket.Stream.IPv4

import qualified String.Ascii as S

newtype Kafka = Kafka { getKafka :: Connection }

instance Show Kafka where
  show _ = "<Kafka>"

data Topic = Topic
  {-# UNPACK #-} !TopicName -- Topic name
  {-# UNPACK #-} !Int -- Number of partitions
  {-# UNPACK #-} !(IORef Int) -- incrementing number

newtype TopicName = TopicName S.String
  deriving newtype (Eq, Ord, Show, IsString)

getTopicName :: Topic -> TopicName
getTopicName (Topic name _ _) = name

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

deriving stock instance Show KafkaException

data OffsetCommitErrorMessage = OffsetCommitErrorMessage
  { commitErrorTopic :: {-# UNPACK #-} !TopicName
  , commitErrorPartition :: {-# UNPACK #-} !Int32
  , commitErrorCode :: {-# UNPACK #-} !Int16
  } deriving Show

data FetchErrorMessage = FetchErrorMessage
  { fetchErrorTopic :: {-# UNPACK #-} !TopicName
  , fetchErrorPartition :: {-# UNPACK #-} !Int32
  , fetchErrorCode :: {-# UNPACK #-} !Int16
  } deriving Show

data GroupMember = GroupMember
  { nameThis0 :: {-# UNPACK #-} !ByteArray
  , nameThis1 :: !(Maybe ByteArray)
  }
  deriving (Eq, Show)

data GenerationId = GenerationId
  { getGenerationId :: {-# UNPACK #-} !Int32
  } deriving (Eq, Show)

data MemberAssignment = MemberAssignment
  { assignedMemberId :: {-# UNPACK #-} !ByteArray
  , assignedTopics :: [TopicAssignment]
  } deriving (Eq, Show)

data TopicAssignment = TopicAssignment
  { assignedTopicName :: !TopicName
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

clientId :: S.String
clientId = "ruko"

clientIdLength :: Int
clientIdLength = S.length clientId

correlationId :: Int32
correlationId = 0xbeef

magic :: Int8
magic = 2

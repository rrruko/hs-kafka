module Kafka.SyncGroup.Response
  ( SyncGroupResponse(..)
  , SyncMemberAssignment(..)
  , getSyncGroupResponse
  , parseSyncGroupResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>), takeByteString)
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data SyncGroupResponse = SyncGroupResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  , memberAssignment :: SyncMemberAssignment
  } deriving (Eq, Show)

data SyncMemberAssignment = SyncMemberAssignment
  { version :: Int16
  , partitionAssignments :: [SyncTopicAssignment]
  , userData :: ByteString
  } deriving (Eq, Show)

data SyncTopicAssignment = SyncTopicAssignment
  { assignedTopic :: ByteString
  , assignedPartitions :: [Int32]
  } deriving (Eq, Show)

parseTopicPartitions :: Parser SyncTopicAssignment
parseTopicPartitions =
  SyncTopicAssignment
    <$> (byteString <?> "assigned topic")
    <*> (array int32 <?> "assigned partitions")

parseMemberAssignment :: Parser SyncMemberAssignment
parseMemberAssignment = do
  _bytesSize <- int32
  SyncMemberAssignment
    <$> (int16 <?> "assignment version")
    <*> (array parseTopicPartitions <?> "topic partitions")
    <*> (takeByteString <?> "user data")

parseSyncGroupResponse :: Parser SyncGroupResponse
parseSyncGroupResponse = do
  _correlationId <- int32 <?> "correlation id"
  SyncGroupResponse
    <$> (int32 <?> "throttle time")
    <*> (int16 <?> "error code")
    <*> (parseMemberAssignment <?> "member assignment")

getSyncGroupResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String SyncGroupResponse))
getSyncGroupResponse = fromKafkaResponse parseSyncGroupResponse

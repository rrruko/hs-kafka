{-# language
    BangPatterns
  #-}

module Kafka.Internal.SyncGroup.Response
  ( SyncGroupResponse(..)
  , SyncMemberAssignment(..)
  , SyncTopicAssignment(..)
  , getSyncGroupResponse
  , parseSyncGroupResponse
  ) where

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data SyncGroupResponse = SyncGroupResponse
  { throttleTimeMs :: !Int32
  , errorCode :: !Int16
  , memberAssignment :: !(Maybe SyncMemberAssignment)
  } deriving (Eq, Show)

data SyncMemberAssignment = SyncMemberAssignment
  { version :: !Int16
  , partitionAssignments :: [SyncTopicAssignment]
  , userData :: !ByteArray
  } deriving (Eq, Show)

data SyncTopicAssignment = SyncTopicAssignment
  { topic :: !TopicName
  , partitions :: [Int32]
  } deriving (Eq, Show)

parseTopicPartitions :: Parser SyncTopicAssignment
parseTopicPartitions = SyncTopicAssignment
  <$> (topicName <?> "assigned topic")
  <*> (array (int32 "assigned partitions")  <?> "assigned partitions")

parseMemberAssignment :: Parser SyncMemberAssignment
parseMemberAssignment = SyncMemberAssignment
  <$> (int16 "assignment version")
  <*> (array parseTopicPartitions <?> "topic partitions")
  <*> (takeByteArray <?> "user data")

parseSyncGroupResponse :: Parser SyncGroupResponse
parseSyncGroupResponse = do
  _correlationId <- int32 "correlation id"
  SyncGroupResponse
    <$> (int32 "throttle time")
    <*> (int16 "error code")
    <*> (nullableBytes parseMemberAssignment <?> "member assignment")

getSyncGroupResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String SyncGroupResponse))
getSyncGroupResponse = fromKafkaResponse parseSyncGroupResponse

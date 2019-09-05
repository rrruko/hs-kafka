{-# OPTIONS -fno-warn-orphans #-}

module Kafka.Internal.Request.Types where

import Data.Int (Int32)
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Kafka.Common

data ProduceRequest = ProduceRequest
  { produceTopic :: Topic -- current state of the topic we're producing to
  , produceWaitTime :: Int -- number of microseconds to wait for response
  , producePayloads :: UnliftedArray ByteArray -- payloads
  } deriving Show

instance Show Topic where
  show (Topic topicName parts _) =
    "Topic " <> show (toByteString topicName) <> " " <> show parts

data FetchRequest = FetchRequest
  { fetchTopic :: TopicName
  , fetchWaitTime :: Int
  , fetchPartitionOffsets :: [PartitionOffset]
  , fetchMaxBytes :: Int32
  } deriving Show

data ListOffsetsRequest = ListOffsetsRequest
  { listOffsetsTopic :: TopicName
  , listOffsetsIndices :: [Int32]
  , listOffsetsTimestamp :: KafkaTimestamp
  } deriving Show
  
data JoinGroupRequest = JoinGroupRequest
  { joinGroupTopic :: TopicName
  , joinGroupMember :: GroupMember
  } deriving Show

data HeartbeatRequest = HeartbeatRequest 
  { heartbeatMember :: GroupMember
  , heartbeatGenId :: GenerationId
  } deriving Show

data SyncGroupRequest = SyncGroupRequest
  { syncGroupMember :: GroupMember
  , syncGroupGenId :: GenerationId
  , syncGroupAssignments :: [MemberAssignment]
  } deriving Show

data OffsetCommitRequest = OffsetCommitRequest
  { offsetCommitTopic :: TopicName
  , offsetCommitOffsets :: [PartitionOffset]
  , offsetCommitMember :: GroupMember
  , offsetCommitGenId :: GenerationId
  } deriving Show

data OffsetFetchRequest = OffsetFetchRequest
  { offsetFetchTopic :: TopicName
  , offsetFetchMember :: GroupMember
  , offsetFetchIndices :: [Int32]
  } deriving Show

data LeaveGroupRequest = LeaveGroupRequest
  { leaveGroupMember :: GroupMember
  } deriving Show

data MetadataRequest = MetadataRequest
  { metadataTopic :: TopicName
  , metadataAutoCreateTopic :: AutoCreateTopic
  } deriving Show




{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-orphans #-}

module Kafka.Internal.Request.Types where

import Data.Primitive.Unlifted.Array
import Kafka.Common
import Kafka.Internal.ShowDebug

import qualified Data.List as L

data ProduceRequest = ProduceRequest
  { produceTopic :: !Topic
    -- ^ current state of the topic we're producing to
  , produceWaitTime :: {-# UNPACK #-} !Int
    -- ^ number of microseconds to wait for response
  , producePayloads :: !(UnliftedArray ByteArray)
    -- ^ payloads
  }

instance ShowDebug ProduceRequest where
  showDebug ProduceRequest{..} = L.unlines
    [ "Produce Request"
    , "  topic: " <> showDebug produceTopic
    , "  wait time: " <> show produceWaitTime
    , "  payloads: <payloads>"
    ]

data FetchRequest = FetchRequest
  { fetchTopic :: {-# UNPACK #-} !TopicName
  , fetchWaitTime :: {-# UNPACK #-} !Int
  , fetchPartitionOffsets :: [PartitionOffset]
  , fetchMaxBytes :: {-# UNPACK #-} !Int32
  }

instance ShowDebug FetchRequest where
  showDebug FetchRequest{..} = L.unlines
    [ "Fetch Request"
    , "  topic: " <> showDebug fetchTopic
    , "  wait time: " <> show fetchWaitTime
    , "  partition offsets: " <> show fetchPartitionOffsets
    , "  max bytes: " <> show fetchMaxBytes
    ]

data ListOffsetsRequest = ListOffsetsRequest
  { listOffsetsTopic :: {-# UNPACK #-} !TopicName
  , listOffsetsIndices :: [Int32]
  , listOffsetsTimestamp :: !KafkaTimestamp
  }

instance ShowDebug ListOffsetsRequest where
  showDebug ListOffsetsRequest{..} = L.unlines
    [ "ListOffsets Request"
    , "  topic: " <> showDebug listOffsetsTopic
    , "  indices: " <> show listOffsetsIndices
    , "  timestamp: " <> show listOffsetsTimestamp
    ]

data JoinGroupRequest = JoinGroupRequest
  { joinGroupTopic :: {-# UNPACK #-} !TopicName
  , joinGroupMember :: !GroupMember
  }

instance ShowDebug JoinGroupRequest where
  showDebug JoinGroupRequest{..} = L.unlines
    [ "JoinGroup Request"
    , "  topic: " <> showDebug joinGroupTopic
    , "  member: " <> showDebug joinGroupMember
    ]

data FindCoordinatorRequest = FindCoordinatorRequest
  { findCoordinatorKey :: {-# UNPACK #-} !ByteArray
  , findCoordinatorKeyType :: {-# UNPACK #-} !Int8
  }

instance ShowDebug FindCoordinatorRequest where
  showDebug FindCoordinatorRequest{..} = L.unlines
    [ "FindCoordinator Request"
    , "  key: " <> showDebug findCoordinatorKey
    , "  key_type: " <> showDebug findCoordinatorKeyType
    ]

data HeartbeatRequest = HeartbeatRequest
  { heartbeatMember :: !GroupMember
  , heartbeatGenId :: !GenerationId
  }

instance ShowDebug HeartbeatRequest where
  showDebug HeartbeatRequest{..} = L.unlines
    [ "Heartbeat Request"
    , "  member: " <> showDebug heartbeatMember
    , "  generation id: " <> show heartbeatGenId
    ]

data SyncGroupRequest = SyncGroupRequest
  { syncGroupMember :: !GroupMember
  , syncGroupGenId :: !GenerationId
  , syncGroupAssignments :: [MemberAssignment]
  }

instance ShowDebug SyncGroupRequest where
  showDebug SyncGroupRequest{..} = L.unlines
    [ "SyncGroup Request"
    , "  member: " <> showDebug syncGroupMember
    , "  generation id: " <> show syncGroupGenId
    , "  assignments: " <> showDebug syncGroupAssignments
    ]

data OffsetCommitRequest = OffsetCommitRequest
  { offsetCommitTopic :: {-# UNPACK #-} !TopicName
  , offsetCommitOffsets :: [PartitionOffset]
  , offsetCommitMember :: !GroupMember
  , offsetCommitGenId :: !GenerationId
  }

instance ShowDebug OffsetCommitRequest where
  showDebug OffsetCommitRequest{..} = L.unlines
    [ "OffsetCommit Request"
    , "  topic: " <> showDebug offsetCommitTopic
    , "  offsets: " <> show offsetCommitOffsets
    , "  member: " <> showDebug offsetCommitMember
    , "  generation id: " <> show offsetCommitGenId
    ]

data OffsetFetchRequest = OffsetFetchRequest
  { offsetFetchTopic :: {-# UNPACK #-} !TopicName
  , offsetFetchMember :: !GroupMember
  , offsetFetchIndices :: [Int32]
  }

instance ShowDebug OffsetFetchRequest where
  showDebug OffsetFetchRequest{..} = L.unlines
    [ "OffsetFetch Request"
    , "  topic: " <> showDebug offsetFetchTopic
    , "  member: " <> showDebug offsetFetchMember
    , "  indices: " <> show offsetFetchIndices
    ]

data LeaveGroupRequest = LeaveGroupRequest
  { leaveGroupMember :: !GroupMember
  }

instance ShowDebug LeaveGroupRequest where
  showDebug LeaveGroupRequest{..} = L.unlines
    [ "LeaveGroup Request"
    , "  member: " <> showDebug leaveGroupMember
    ]

data MetadataRequest = MetadataRequest
  { metadataTopic :: {-# UNPACK #-} !TopicName
  , metadataAutoCreateTopic :: !AutoCreateTopic
  }

instance ShowDebug MetadataRequest where
  showDebug MetadataRequest{..} = L.unlines
    [ "Metadata Request"
    , "  topic: " <> showDebug metadataTopic
    , "  auto create topic: " <> show metadataAutoCreateTopic
    ]

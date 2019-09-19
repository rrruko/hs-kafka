{-# language
    BangPatterns
  , OverloadedStrings
  #-}

module Kafka.Internal.JoinGroup.Response
  ( JoinGroupResponse(..)
  , Member(..)
  , getJoinGroupResponse
  , parseJoinGroupResponse
  ) where

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response

data JoinGroupResponse = JoinGroupResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , generationId :: {-# UNPACK #-} !Int32
  , groupProtocol :: {-# UNPACK #-} !ByteArray
  , leaderId :: {-# UNPACK #-} !ByteArray
  , memberId :: {-# UNPACK #-} !ByteArray
  , members :: [Member]
  } deriving (Eq, Show)

data Member = Member
  { groupMemberId :: {-# UNPACK #-} !ByteArray
  , groupMemberMetadata :: {-# UNPACK #-} !ByteArray
  } deriving (Eq, Show)

parseJoinGroupResponse :: Parser JoinGroupResponse
parseJoinGroupResponse = do
  _correlationId <- int32 "correlation id"
  JoinGroupResponse
    <$> (int32 "throttle time")
    <*> (int16 "error code")
    <*> (int32 "generation id")
    <*> (bytearray) -- "group protocol")
    <*> (bytearray) -- "leader id")
    <*> (bytearray) -- "member id")
    <*> (array parseMember) -- "members")

parseMember :: Parser Member
parseMember = Member
  <$> (bytearray) -- <?> "member id")
  <*> (sizedBytes) -- <?> "member metadata")

getJoinGroupResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String JoinGroupResponse))
getJoinGroupResponse = fromKafkaResponse parseJoinGroupResponse

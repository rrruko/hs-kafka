{-# language
    BangPatterns
  #-}

module Kafka.Internal.LeaveGroup.Response
  ( LeaveGroupResponse(..)
  , getLeaveGroupResponse
  , parseLeaveGroupResponse
  ) where

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data LeaveGroupResponse = LeaveGroupResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseLeaveGroupResponse :: Parser LeaveGroupResponse
parseLeaveGroupResponse = do
  _correlationId <- int32 "correlation id"
  LeaveGroupResponse
    <$> (int32 "throttle time")
    <*> (int16 "error code")

getLeaveGroupResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String LeaveGroupResponse))
getLeaveGroupResponse = fromKafkaResponse parseLeaveGroupResponse

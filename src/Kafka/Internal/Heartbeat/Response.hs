{-# language
    BangPatterns
  #-}

module Kafka.Internal.Heartbeat.Response
  ( HeartbeatResponse(..)
  , getHeartbeatResponse
  , parseHeartbeatResponse
  ) where

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response

data HeartbeatResponse = HeartbeatResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseHeartbeatResponse :: Parser HeartbeatResponse
parseHeartbeatResponse = do
  _correlationId <- int32 "correlation id"
  HeartbeatResponse
    <$> (int32 "throttle time")
    <*> (int16 "error code")

getHeartbeatResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String HeartbeatResponse))
getHeartbeatResponse = fromKafkaResponse parseHeartbeatResponse

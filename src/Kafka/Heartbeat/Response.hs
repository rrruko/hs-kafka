module Kafka.Heartbeat.Response
  ( HeartbeatResponse(..)
  , getHeartbeatResponse
  , parseHeartbeatResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data HeartbeatResponse = HeartbeatResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  } deriving (Eq, Show)

parseHeartbeatResponse :: Parser HeartbeatResponse
parseHeartbeatResponse = do
  _correlationId <- int32 <?> "correlation id"
  HeartbeatResponse
    <$> (int32 <?> "throttle time")
    <*> (int16 <?> "error code")

getHeartbeatResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String HeartbeatResponse))
getHeartbeatResponse = fromKafkaResponse parseHeartbeatResponse

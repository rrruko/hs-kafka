module Kafka.LeaveGroup.Response
  ( LeaveGroupResponse(..)
  , getLeaveGroupResponse
  , parseLeaveGroupResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data LeaveGroupResponse = LeaveGroupResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  } deriving (Eq, Show)

parseLeaveGroupResponse :: Parser LeaveGroupResponse
parseLeaveGroupResponse = do
  _correlationId <- int32 <?> "correlation id"
  LeaveGroupResponse
    <$> (int32 <?> "throttle time")
    <*> (int16 <?> "error code")

getLeaveGroupResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String LeaveGroupResponse))
getLeaveGroupResponse = fromKafkaResponse parseLeaveGroupResponse

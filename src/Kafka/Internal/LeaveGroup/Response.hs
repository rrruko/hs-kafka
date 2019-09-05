module Kafka.Internal.LeaveGroup.Response
  ( LeaveGroupResponse(..)
  , getLeaveGroupResponse
  , parseLeaveGroupResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.Int
import GHC.Conc
import System.IO

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

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
  -> Maybe Handle
  -> IO (Either KafkaException (Either String LeaveGroupResponse))
getLeaveGroupResponse = fromKafkaResponse parseLeaveGroupResponse

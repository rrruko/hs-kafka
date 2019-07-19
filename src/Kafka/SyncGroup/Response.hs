module Kafka.SyncGroup.Response
  ( SyncGroupResponse(..)
  , getSyncGroupResponse
  , parseSyncGroupResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data SyncGroupResponse = SyncGroupResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  , memberAssignment :: ByteString
  } deriving (Eq, Show)

parseSyncGroupResponse :: Parser SyncGroupResponse
parseSyncGroupResponse = do
  _correlationId <- int32 <?> "correlation id"
  SyncGroupResponse
    <$> (int32 <?> "throttle time")
    <*> (int16 <?> "error code")
    <*> (byteString <?> "member assignment")

getSyncGroupResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String SyncGroupResponse))
getSyncGroupResponse = fromKafkaResponse parseSyncGroupResponse

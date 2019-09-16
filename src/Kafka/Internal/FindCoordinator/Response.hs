{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.FindCoordinator.Response
  ( FindCoordinatorResponse(..)
  , getFindCoordinatorResponse
  , parseFindCoordinatorResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc
import System.IO

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response

data FindCoordinatorResponse = FindCoordinatorResponse
  { throttleTimeMs :: !Int32
  , errorCode :: !Int16
  , errorMessage :: Maybe ByteString
  , node_id :: !Int32
  , host :: ByteString
  , port :: !Int32
  } deriving (Eq, Show)

parseFindCoordinatorResponse :: Parser FindCoordinatorResponse
parseFindCoordinatorResponse = do
  _correlationId <- int32 <?> "correlation id"
  FindCoordinatorResponse
    <$> (int32 <?> "throttle time")
    <*> (int16 <?> "error code")
    <*> (nullableByteString <?> "generation id")
    <*> (int32 <?> "group protocol")
    <*> (byteString <?> "leader id")
    <*> (int32 <?> "member id")

getFindCoordinatorResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String FindCoordinatorResponse))
getFindCoordinatorResponse = fromKafkaResponse parseFindCoordinatorResponse


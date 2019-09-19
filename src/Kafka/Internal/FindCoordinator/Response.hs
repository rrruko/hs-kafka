{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.FindCoordinator.Response
  ( FindCoordinatorResponse(..)
  , getFindCoordinatorResponse
  , parseFindCoordinatorResponse
  ) where

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response

data FindCoordinatorResponse = FindCoordinatorResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , errorMessage :: !(Maybe ByteArray)
  , node_id :: {-# UNPACK #-} !Int32
  , host :: {-# UNPACK #-} !ByteArray
  , port :: {-# UNPACK #-} !Int32
  } deriving (Eq, Show)

parseFindCoordinatorResponse :: Parser FindCoordinatorResponse
parseFindCoordinatorResponse = do
  _correlationId <- int32 "correlation id"
  FindCoordinatorResponse
    <$> (int32 "throttle time")
    <*> (int16 "error code")
    <*> (nullableByteArray) -- <?> "generation id")
    <*> (int32 "group protocol")
    <*> (bytearray) -- <?> "leader id")
    <*> (int32 "member id")

getFindCoordinatorResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String FindCoordinatorResponse))
getFindCoordinatorResponse = fromKafkaResponse parseFindCoordinatorResponse


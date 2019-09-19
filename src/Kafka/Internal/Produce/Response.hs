{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  #-}

module Kafka.Internal.Produce.Response
  ( ProducePartitionResponse(..)
  , ProduceResponse(..)
  , ProduceResponseMessage(..)
  , getProduceResponse
  , parseProduceResponse
  ) where

import System.IO (Handle)

import Kafka.Internal.Combinator
import Kafka.Common (Kafka, KafkaException(..), TopicName(..))
import Kafka.Internal.Response (fromKafkaResponse)

import qualified Data.Bytes as B
import qualified Data.Bytes.Parser as Smith
import qualified String.Ascii as S

data ProduceResponse = ProduceResponse
  { produceResponseMessages :: [ProduceResponseMessage]
  , throttleTimeMs :: !Int32
  } deriving (Eq, Show)

data ProduceResponseMessage = ProduceResponseMessage
  { prMessageTopic :: !TopicName
  , prPartitionResponses :: [ProducePartitionResponse]
  } deriving (Eq, Show)

data ProducePartitionResponse = ProducePartitionResponse
  { prResponsePartition :: !Int32
  , prResponseErrorCode :: !Int16
  , prResponseBaseOffset :: !Int64
  , prResponseLogAppendTime :: !Int64
  , prResponseLogStartTime :: !Int64
  } deriving (Eq, Show)

parseProduceResponse :: Parser ProduceResponse
parseProduceResponse = do
  -- we need to consume this, but we discard it. (why?)
  _correlationId <- int32 "correlationId"
  responsesCount <- int32 "responses count"
  ProduceResponse
    <$> (count responsesCount parseProduceResponseMessage) -- response messages
    <*> (int32 "throttle time")

parseProduceResponseMessage :: Parser ProduceResponseMessage
parseProduceResponseMessage = do
  tlen <- int16 "topic length"
  t <- Smith.take "topic name" (fromIntegral tlen)
  case S.fromByteArray (B.toByteArray t) of
    Nothing -> fail "produce response message: non-ascii topic name"
    Just top -> do
      prc <- int32 "partition response count"
      resps <- count prc parseProducePartitionResponse
      pure (ProduceResponseMessage (TopicName top) resps)

parseProducePartitionResponse :: Parser ProducePartitionResponse
parseProducePartitionResponse = ProducePartitionResponse
  <$> int32 "int32"
  <*> int16 "int16"
  <*> int64 "int64"
  <*> int64 "int64"
  <*> int64 "int64"

getProduceResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String ProduceResponse))
getProduceResponse = fromKafkaResponse parseProduceResponse

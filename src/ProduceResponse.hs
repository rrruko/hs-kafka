{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module ProduceResponse
  ( ProduceResponse(..)
  , ProduceResponseMessage(..)
  , ProducePartitionResponse(..)
  , getProduceResponse
  ) where

import Data.Attoparsec.ByteString ((<?>), Parser)
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc

import qualified Data.Attoparsec.ByteString as AT

import Combinator
import Common
import KafkaResponse

data ProduceResponse = ProduceResponse
  { produceResponseMessages :: [ProduceResponseMessage]
  , throttleTimeMs :: Int32
  } deriving Show

data ProduceResponseMessage = ProduceResponseMessage
  { prMessageTopic :: ByteString
  , prPartitionResponses :: [ProducePartitionResponse]
  } deriving Show

data ProducePartitionResponse = ProducePartitionResponse
  { prResponsePartition :: Int32
  , prResponseErrorCode :: Int16
  , prResponseBaseOffset :: Int64
  , prResponseLogAppendTime :: Int64
  , prResponseLogStartTime :: Int64
  } deriving Show

parseProduceResponse :: Parser ProduceResponse
parseProduceResponse = do
  _correlationId <- int32 <?> "correlation id"
  responsesCount <- int32 <?> "responses count"
  ProduceResponse
    <$> (count responsesCount parseProduceResponseMessage
          <?> "response messages")
    <*> (int32 <?> "throttle time")

parseProduceResponseMessage :: Parser ProduceResponseMessage
parseProduceResponseMessage = do
  topicLengthBytes <- int16 <?> "topic length"
  topicName <- AT.take (fromIntegral topicLengthBytes) <?> "topic name"
  partitionResponseCount <- int32 <?> "partition response count"
  responses <- count partitionResponseCount parseProducePartitionResponse
    <?> "responses"
  pure (ProduceResponseMessage topicName responses)

parseProducePartitionResponse :: Parser ProducePartitionResponse
parseProducePartitionResponse = ProducePartitionResponse
  <$> int32
  <*> int16
  <*> int64
  <*> int64
  <*> int64

getProduceResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String ProduceResponse))
getProduceResponse kafka interrupt =
  (fmap . fmap)
    (AT.parseOnly parseProduceResponse)
    (getKafkaResponse kafka interrupt)

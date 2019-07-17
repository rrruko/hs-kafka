{-# LANGUAGE OverloadedStrings #-}

module Kafka.ListOffsets.Response
  ( ListOffsetsResponse(..)
  , ListOffsetsResponseMessage(..)
  , PartitionResponse(..)
  , getListOffsetsResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data ListOffsetsResponse = ListOffsetsResponse
  { throttleTimeMs :: Int32
  , responses :: [ListOffsetsResponseMessage]
  } deriving (Eq, Show)

data ListOffsetsResponseMessage = ListOffsetsResponseMessage
  { topic :: ByteString
  , partitionResponses :: [PartitionResponse]
  } deriving (Eq, Show)

data PartitionResponse = PartitionResponse
  { partition :: Int32
  , errorCode :: Int16
  , timestamp :: Int64
  , offset :: Int64
  , leaderEpoch :: Int32
  } deriving (Eq, Show)

parseListOffsetsResponse :: Parser ListOffsetsResponse
parseListOffsetsResponse = do
  _correlationId <- int32 <?> "correlation id"
  ListOffsetsResponse
    <$> int32
    <*> array parseListOffsetsResponseMessage

parseListOffsetsResponseMessage :: Parser ListOffsetsResponseMessage
parseListOffsetsResponseMessage =
  ListOffsetsResponseMessage
    <$> byteString
    <*> array parsePartitionResponse

parsePartitionResponse :: Parser PartitionResponse
parsePartitionResponse =
  PartitionResponse
    <$> int32
    <*> int16
    <*> int64
    <*> int64
    <*> int32

getListOffsetsResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String ListOffsetsResponse))
getListOffsetsResponse = fromKafkaResponse parseListOffsetsResponse

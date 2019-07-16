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

import qualified Data.Attoparsec.ByteString as AT

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data ListOffsetsResponse = ListOffsetsResponse
  { throttleTimeMs :: Int32
  , responses :: [ListOffsetsResponseMessage]
  } deriving Show

data ListOffsetsResponseMessage = ListOffsetsResponseMessage
  { topic :: ByteString
  , partitionResponses :: [PartitionResponse]
  } deriving Show

data PartitionResponse = PartitionResponse
  { partition :: Int32
  , errorCode :: Int16
  , timestamp :: Int64
  , offset :: Int64
  , leaderEpoch :: Int32
  } deriving Show

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
getListOffsetsResponse kafka interrupt =
  (fmap . fmap)
    (AT.parseOnly parseListOffsetsResponse)
    (getKafkaResponse kafka interrupt)

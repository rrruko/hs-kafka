{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.ListOffsets.Response
  ( ListOffsetsResponse(..)
  , ListOffsetsTopic(..)
  , ListOffsetPartition(..)
  , getListOffsetsResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data ListOffsetsResponse = ListOffsetsResponse
  { throttleTimeMs :: Int32
  , topics :: [ListOffsetsTopic]
  } deriving (Eq, Show)

data ListOffsetsTopic = ListOffsetsTopic
  { topic :: ByteString
  , partitions :: [ListOffsetPartition]
  } deriving (Eq, Show)

data ListOffsetPartition = ListOffsetPartition
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
    <*> array parseListOffsetsTopic

parseListOffsetsTopic :: Parser ListOffsetsTopic
parseListOffsetsTopic =
  ListOffsetsTopic
    <$> byteString
    <*> array parseListOffsetPartition

parseListOffsetPartition :: Parser ListOffsetPartition
parseListOffsetPartition =
  ListOffsetPartition
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

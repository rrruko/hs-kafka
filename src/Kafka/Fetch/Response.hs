{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Fetch.Response
  ( FetchResponse(..)
  , FetchResponseMessage(..)
  , Header(..)
  , PartitionHeader(..)
  , PartitionResponse(..)
  , Record(..)
  , RecordBatch(..)
  , getFetchResponse
  , parseFetchResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data FetchResponse = FetchResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  , sessionId :: Int32
  , responses :: [FetchResponseMessage]
  } deriving (Eq, Show)

data FetchResponseMessage = FetchResponseMessage
  { fetchResponseTopic :: ByteString
  , partitionResponses :: [PartitionResponse]
  } deriving (Eq, Show)

data PartitionResponse = PartitionResponse
  { partitionHeader :: PartitionHeader
  , recordSet :: Maybe [RecordBatch]
  } deriving (Eq, Show)

data PartitionHeader = PartitionHeader
  { partition :: Int32
  , partitionHeaderErrorCode :: Int16
  , highWatermark :: Int64
  , lastStableOffset :: Int64
  , logStartOffset :: Int64
  , abortedTransactions :: [AbortedTransaction]
  } deriving (Eq, Show)

data AbortedTransaction = AbortedTransaction
  { abortedTransactionProducerId :: Int64
  , firstOffset :: Int64
  } deriving (Eq, Show)

data RecordBatch = RecordBatch
  { baseOffset :: Int64
  , batchLength :: Int32
  , partitionLeaderEpoch :: Int32
  , magic :: Int8
  , crc :: Int32
  , attributes :: Int16
  , lastOffsetDelta :: Int32
  , firstTimestamp :: Int64
  , maxTimestamp :: Int64
  , producerId :: Int64
  , producerEpoch :: Int16
  , baseSequence :: Int32
  , records :: [Record]
  } deriving (Eq, Show)

data Record = Record
  { recordLength :: Int
  , recordAttributes :: Int8
  , recordTimestampDelta :: Int
  , recordOffsetDelta :: Int
  , recordKeyLength :: Int
  , recordKey :: Maybe ByteString
  , recordValueLength :: Int
  , recordValue :: Maybe ByteString
  , recordHeaders :: [Header]
  } deriving (Eq, Show)

data Header = Header
  { headerKeyLength :: Int
  , headerKey :: Maybe ByteString
  , headerValueLength :: Int
  , headerValue :: Maybe ByteString
  } deriving (Eq, Show)

parseFetchResponse :: Parser FetchResponse
parseFetchResponse = do
  _correlationId <- int32 <?> "correlation id"
  FetchResponse
    <$> int32
    <*> int16
    <*> int32
    <*> nullableArray parseFetchResponseMessage

parseFetchResponseMessage :: Parser FetchResponseMessage
parseFetchResponseMessage = do
  topicName <- byteString <?> "topic name"
  rs <- nullableArray parsePartitionResponse
  pure (FetchResponseMessage topicName rs)

parsePartitionResponse :: Parser PartitionResponse
parsePartitionResponse = PartitionResponse
  <$> (parsePartitionHeader <?> "partition header")
  <*> (nullableSequence parseRecordBatch <?> "record batch")

parsePartitionHeader :: Parser PartitionHeader
parsePartitionHeader = PartitionHeader
  <$> (int32 <?> "partition")
  <*> (int16 <?> "error code")
  <*> (int64 <?> "high watermark")
  <*> (int64 <?> "last stable offset")
  <*> (int64 <?> "log start offset")
  <*> (nullableArray parseAbortedTransaction <?> "aborted transactions")

parseAbortedTransaction :: Parser AbortedTransaction
parseAbortedTransaction = AbortedTransaction
  <$> (int64 <?> "producer id")
  <*> (int64 <?> "first offset")

parseRecordBatch :: Parser RecordBatch
parseRecordBatch =
  RecordBatch
    <$> int64
    <*> int32
    <*> int32
    <*> int8
    <*> int32
    <*> int16
    <*> int32
    <*> int64
    <*> int64
    <*> int64
    <*> int16
    <*> int32
    <*> nullableArray parseRecord

parseRecord :: Parser Record
parseRecord = do
  recordLength <- parseVarint <?> "record length"
  recordAttributes <- int8 <?> "record attributes"
  recordTimestampDelta <- parseVarint <?> "record timestamp delta"
  recordOffsetDelta <- parseVarint <?> "record offset delta"
  recordKeyLength <- parseVarint <?> "record key length"
  recordKey <- nullableByteString recordKeyLength <?> "record key"
  recordValueLength <- parseVarint <?> "record value length"
  recordValue <- nullableByteString recordValueLength <?> "record value"
  recordHeaders <- varintArray parseHeader <?> "record headers"
  pure (Record {..})

varintArray :: Parser a -> Parser [a]
varintArray p = do
  arraySize <- parseVarint
  count arraySize p

parseHeader :: Parser Header
parseHeader = do
  headerKeyLength <- parseVarint <?> "header key length"
  headerKey <- nullableByteString headerKeyLength <?> "header key"
  headerValueLength <- parseVarint <?> "header value length"
  headerValue <- nullableByteString headerValueLength <?> "header value"
  pure (Header {..})

getFetchResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String FetchResponse))
getFetchResponse = fromKafkaResponse parseFetchResponse

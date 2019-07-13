{-# LANGUAGE LambdaCase #-}

module FetchResponse
  ( FetchResponse(..)
  , FetchResponseMessage(..)
  , PartitionResponse(..)
  , PartitionHeader(..)
  , AbortedTransaction(..)
  , RecordBatch(..)
  , Record(..)
  , Header(..)
  , getFetchResponse
  ) where

import Data.Attoparsec.ByteString (Parser)
import Data.ByteString
import Data.Int
import GHC.Conc

import qualified Data.Attoparsec.ByteString as AT

import Combinator
import Common
import KafkaResponse

data FetchResponse = FetchResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  , sessionId :: Int32
  , responses :: [FetchResponseMessage]
  } deriving Show

data FetchResponseMessage = FetchResponseMessage
  { topic :: ByteString
  , partitionResponses :: [PartitionResponse]
  } deriving Show

data PartitionResponse = PartitionResponse
  { partitionHeader :: PartitionHeader
  , recordSet :: RecordBatch
  } deriving Show

data PartitionHeader = PartitionHeader
  { partition :: Int32
  , partitionHeaderErrorCode :: Int16
  , highWatermark :: Int64
  , lastStableOffset :: Int64
  , logStartOffset :: Int64
  , abortedTransactions :: [AbortedTransaction]
  } deriving Show

data AbortedTransaction = AbortedTransaction
  { abortedTransactionProducerId :: Int64
  , firstOffset :: Int64
  } deriving Show

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
  } deriving Show

data Record = Record
  { recordLength :: Int
  , recordAttributes :: Int8
  , recordTimestampDelta :: Int
  , recordOffsetDelta :: Int
  , recordKeyLength :: Int
  , recordKey :: ByteString
  , recordValueLength :: Int
  , recordValue :: ByteString
  , recordHeaders :: [Header]
  } deriving Show

data Header = Header
  { headerKeyLength :: Int
  , headerKey :: ByteString
  , headerValueLength :: Int
  , headerValue :: ByteString
  } deriving Show

parseFetchResponse :: Parser FetchResponse
parseFetchResponse = FetchResponse
  <$> int32
  <*> int16
  <*> int32
  <*> array parseFetchResponseMessage

parseFetchResponseMessage :: Parser FetchResponseMessage
parseFetchResponseMessage = FetchResponseMessage
  <$> byteString
  <*> array parsePartitionResponse

parsePartitionResponse :: Parser PartitionResponse
parsePartitionResponse = PartitionResponse
  <$> parsePartitionHeader
  <*> parseRecordBatch

parsePartitionHeader :: Parser PartitionHeader
parsePartitionHeader = PartitionHeader
  <$> int32
  <*> int16
  <*> int64
  <*> int64
  <*> int64
  <*> array parseAbortedTransaction

parseAbortedTransaction :: Parser AbortedTransaction
parseAbortedTransaction = AbortedTransaction
  <$> int64
  <*> int64

parseRecordBatch :: Parser RecordBatch
parseRecordBatch = RecordBatch
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
  <*> array parseRecord

parseRecord :: Parser Record
parseRecord = fail "parser unimplemented"

getFetchResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String FetchResponse))
getFetchResponse kafka interrupt =
  (fmap . fmap)
    (AT.parseOnly parseFetchResponse)
    (getKafkaResponse kafka interrupt)

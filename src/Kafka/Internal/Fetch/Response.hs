{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Fetch.Response
  ( FetchResponse(..)
  , FetchTopic(..)
  , Header(..)
  , PartitionHeader(..)
  , FetchPartition(..)
  , Record(..)
  , RecordBatch(..)
  , getFetchResponse
  , parseFetchResponse
  , partitionLastSeenOffset
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString (ByteString)
import Data.Int
import Data.List (find, intercalate)
import Data.List.NonEmpty (nonEmpty)
import GHC.Conc
import System.IO

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response
import Kafka.Internal.ShowDebug

showDebugSeq :: ShowDebug a => [a] -> String
showDebugSeq = intercalate "\n" . fmap showDebug

instance Show FetchResponse where
  show = showDebug

instance ShowDebug FetchResponse where
  showDebug FetchResponse {..} = intercalate "\n"
    [ "Fetch Response"
    , "  throttle time ms: " <> showDebug throttleTimeMs
    , "  error code: " <> showDebug errorCode
    , "  session id: " <> showDebug sessionId
    , "  topics: "
    , showDebugSeq topics
    ]

instance ShowDebug FetchTopic where
  showDebug FetchTopic {..} = intercalate "\n"
    [ "    Topic"
    , "      topic: " <> showDebug topic
    , "      partitions: "
    , showDebugSeq partitions
    ]

instance ShowDebug FetchPartition where
  showDebug FetchPartition {..} = intercalate "\n"
    [ "        Partition"
    , "          partition header: "
    , showDebug partitionHeader
    , "          record set: "
    , case recordSet of
        Nothing -> "            Nothing"
        Just rs -> showDebugSeq rs
    ]

instance ShowDebug PartitionHeader where
  showDebug PartitionHeader {..} = intercalate "\n"
    [ "            Partition Header"
    , "              partition: " <> showDebug partition
    , "              partition header error code: " <> showDebug partitionHeaderErrorCode
    , "              high watermark: " <> showDebug highWatermark
    , "              lastStableOffset: " <> showDebug lastStableOffset
    , "              logStartOffset: " <> showDebug logStartOffset
    , "              abortedTransactions: " <> showDebug abortedTransactions
    ]

instance ShowDebug RecordBatch where
  showDebug RecordBatch {..} = intercalate "\n"
    [ "            Record Batch"
    , "              base offset: " <> showDebug baseOffset
    , "              batch length: " <> showDebug batchLength
    , "              partition leader epoch: " <> showDebug partitionLeaderEpoch
    , "              magic: " <> showDebug recordBatchMagic
    , "              crc: " <> showDebug crc
    , "              attributes: " <> showDebug attributes
    , "              lastOffsetDelta: " <> showDebug lastOffsetDelta
    , "              firstTimestamp: " <> showDebug firstTimestamp
    , "              maxTimestamp: " <> showDebug maxTimestamp
    , "              producer id: " <> showDebug producerId
    , "              producerEpoch: " <> showDebug producerEpoch
    , "              baseSequence: " <> showDebug baseSequence
    , "              records: <" <> showDebug (length records) <> " records>"
    ]

instance ShowDebug AbortedTransaction where
  showDebug AbortedTransaction {..} =
    "( producer id = "
    <> showDebug abortedTransactionProducerId
    <> ", first offset = "
    <> showDebug firstOffset
    <> " )"

data FetchResponse = FetchResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  , sessionId :: Int32
  , topics :: [FetchTopic]
  }

data FetchTopic = FetchTopic
  { topic :: ByteString
  , partitions :: [FetchPartition]
  } deriving (Eq, Show)

data FetchPartition = FetchPartition
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
  , recordBatchMagic :: Int8
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
  , recordKey :: Maybe ByteString
  , recordValue :: Maybe ByteString
  , recordHeaders :: [Header]
  } deriving (Eq, Show)

data Header = Header
  { headerKey :: Maybe ByteString
  , headerValue :: Maybe ByteString
  } deriving (Eq, Show)

parseFetchResponse :: Parser FetchResponse
parseFetchResponse = do
  _correlationId <- int32 <?> "correlation id"
  FetchResponse
    <$> int32
    <*> int16
    <*> int32
    <*> nullableArray parseFetchTopic

parseFetchTopic :: Parser FetchTopic
parseFetchTopic = do
  topicName <- byteString <?> "topic name"
  rs <- nullableArray parseFetchPartition
  pure (FetchTopic topicName rs)

parseFetchPartition :: Parser FetchPartition
parseFetchPartition = FetchPartition
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
  recordKey <- nullableByteStringVar <?> "record key"
  recordValue <- nullableByteStringVar <?> "record value"
  recordHeaders <- varintArray parseHeader <?> "record headers"
  pure (Record {..})

varintArray :: Parser a -> Parser [a]
varintArray p = do
  arraySize <- parseVarint
  count arraySize p

parseHeader :: Parser Header
parseHeader = do
  headerKey <- nullableByteStringVar <?> "header key"
  headerValue <- nullableByteStringVar <?> "header value"
  pure (Header {..})

getFetchResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String FetchResponse))
getFetchResponse = fromKafkaResponse parseFetchResponse

lookupTopic :: [FetchTopic] -> ByteString -> Maybe FetchTopic
lookupTopic responses topicName = find
  (\resp -> topicName == topic resp)
  responses

lookupPartition :: [FetchPartition] -> Int32 -> Maybe FetchPartition
lookupPartition responses pid = find
  (\resp -> pid == partition (partitionHeader resp))
  responses

partitionLastSeenOffset :: FetchResponse -> ByteString -> Int32 -> Maybe Int64
partitionLastSeenOffset fetchResponse topicName partitionId = do
  topic <- lookupTopic (topics fetchResponse) topicName
  partition <- lookupPartition (partitions topic) partitionId
  set <- recordSet partition
  maxMaybe (map recordBatchLastOffset set)
  where
  recordBatchLastOffset rb =
    baseOffset rb + fromIntegral (lastOffsetDelta rb) + 1
  maxMaybe xs = fmap (foldr1 max) (nonEmpty xs)

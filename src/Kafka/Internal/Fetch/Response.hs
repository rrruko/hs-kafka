{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  , RankNTypes
  , RecordWildCards
  #-}

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

import Data.List (find)
import Data.List.NonEmpty (nonEmpty)
import Data.Maybe (mapMaybe)

import qualified Data.Foldable as F

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response

fetchResponseContents :: FetchResponse -> [ByteArray]
fetchResponseContents fetchResponse =
    mapMaybe recordValue
  . concatMap records
  . concat
  . mapMaybe recordSet
  . concatMap partitions
  . topics
  $ fetchResponse

instance Show FetchResponse where
  show resp =
    "Fetch ("
    <> show (length (fetchResponseContents resp))
    <> " records)"

data FetchResponse = FetchResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , sessionId :: {-# UNPACK #-} !Int32
  , topics :: [FetchTopic]
  }

data FetchTopic = FetchTopic
  { topic :: !TopicName
  , partitions :: [FetchPartition]
  } deriving (Eq, Show)

data FetchPartition = FetchPartition
  { partitionHeader :: !PartitionHeader
  , recordSet :: !(Maybe [RecordBatch])
  } deriving (Eq, Show)

data PartitionHeader = PartitionHeader
  { partition :: {-# UNPACK #-} !Int32
  , partitionHeaderErrorCode :: {-# UNPACK #-} !Int16
  , highWatermark :: {-# UNPACK #-} !Int64
  , lastStableOffset :: {-# UNPACK #-} !Int64
  , logStartOffset :: {-# UNPACK #-} !Int64
  , abortedTransactions :: [AbortedTransaction]
  } deriving (Eq, Show)

data AbortedTransaction = AbortedTransaction
  { abortedTransactionProducerId :: {-# UNPACK #-} !Int64
  , firstOffset :: {-# UNPACK #-} !Int64
  } deriving (Eq, Show)

data RecordBatch = RecordBatch
  { baseOffset :: {-# UNPACK #-} !Int64
  , batchLength :: {-# UNPACK #-} !Int32
  , partitionLeaderEpoch :: {-# UNPACK #-} !Int32
  , recordBatchMagic :: {-# UNPACK #-} !Int8
  , crc :: {-# UNPACK #-} !Int32
  , attributes :: {-# UNPACK #-} !Int16
  , lastOffsetDelta :: {-# UNPACK #-} !Int32
  , firstTimestamp :: {-# UNPACK #-} !Int64
  , maxTimestamp :: {-# UNPACK #-} !Int64
  , producerId :: {-# UNPACK #-} !Int64
  , producerEpoch :: {-# UNPACK #-} !Int16
  , baseSequence :: {-# UNPACK #-} !Int32
  , records :: [Record]
  } deriving (Eq, Show)

data Record = Record
  { recordLength :: !Int
  , recordAttributes :: !Int8
  , recordTimestampDelta :: !Int
  , recordOffsetDelta :: !Int
  , recordKey :: !(Maybe ByteArray)
  , recordValue :: !(Maybe ByteArray)
  , recordHeaders :: [Header]
  } deriving (Eq, Show)

data Header = Header
  { headerKey :: !(Maybe ByteArray)
  , headerValue :: !(Maybe ByteArray)
  } deriving (Eq, Show)

parseFetchResponse :: Parser FetchResponse
parseFetchResponse = do
  _correlationId <- int32 "correlation id"
  FetchResponse
    <$> int32 "throttleTimeMs"
    <*> int16 "errorCode"
    <*> int32 "sessionId"
    <*> nullableArray parseFetchTopic

parseFetchTopic :: Parser FetchTopic
parseFetchTopic = do
  t <- topicName <?> "topic name"
  rs <- nullableArray parseFetchPartition
  pure (FetchTopic t rs)

parseFetchPartition :: Parser FetchPartition
parseFetchPartition = FetchPartition
  <$> (parsePartitionHeader <?> "partition header")
  <*> (nullableSequence parseRecordBatch <?> "record batch")

parsePartitionHeader :: Parser PartitionHeader
parsePartitionHeader = PartitionHeader
  <$> (int32 "partition")
  <*> (int16 "error code")
  <*> (int64 "high watermark")
  <*> (int64 "last stable offset")
  <*> (int64 "log start offset")
  <*> (nullableArray parseAbortedTransaction <?> "aborted transactions")

parseAbortedTransaction :: Parser AbortedTransaction
parseAbortedTransaction = AbortedTransaction
  <$> (int64 "producer id")
  <*> (int64 "first offset")

parseRecordBatch :: Parser RecordBatch
parseRecordBatch = RecordBatch
  <$> int64 "baseOffset"
  <*> int32 "batchLength"
  <*> int32 "partitionLeaderEpoch"
  <*> int8 "recordBatchMagic"
  <*> int32 "crc"
  <*> int16 "attributes"
  <*> int32 "lastOffsetDelta"
  <*> int64 "firstTiemstamp"
  <*> int64 "maxTimestamp"
  <*> int64 "producerId"
  <*> int16 "producerEpoch"
  <*> int32 "baseSequence"
  <*> nullableArray parseRecord

parseRecord :: Parser Record
parseRecord = do
  recordLength <- varInt <?> "record length"
  recordAttributes <- int8 "record attributes"
  recordTimestampDelta <- varInt <?> "record timestamp delta"
  recordOffsetDelta <- varInt <?> "record offset delta"
  recordKey <- nullableByteArrayVar <?> "record key"
  recordValue <- nullableByteArrayVar <?> "record value"
  recordHeaders <- varintArray parseHeader <?> "record headers"
  pure (Record {..})

varintArray :: Parser a -> Parser [a]
varintArray p = do
  arraySize <- varInt
  count arraySize p

parseHeader :: Parser Header
parseHeader = do
  headerKey <- nullableByteArrayVar <?> "header key"
  headerValue <- nullableByteArrayVar <?> "header value"
  pure (Header {..})

getFetchResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String FetchResponse))
getFetchResponse = fromKafkaResponse parseFetchResponse

lookupTopic :: [FetchTopic] -> TopicName -> Maybe FetchTopic
lookupTopic responses t = find
  (\resp -> t == topic resp)
  responses

lookupPartition :: [FetchPartition] -> Int32 -> Maybe FetchPartition
lookupPartition responses pid = find
  (\resp -> pid == partition (partitionHeader resp))
  responses

partitionLastSeenOffset :: FetchResponse -> TopicName -> Int32 -> Maybe Int64
partitionLastSeenOffset fetchResponse t partitionId = do
  topic <- lookupTopic (topics fetchResponse) t
  partition <- lookupPartition (partitions topic) partitionId
  set <- recordSet partition
  maxMaybe (fmap recordBatchLastOffset set)
  where
  recordBatchLastOffset rb =
    baseOffset rb + fromIntegral (lastOffsetDelta rb) + 1
  maxMaybe xs = fmap (F.foldr1 max) (nonEmpty xs)

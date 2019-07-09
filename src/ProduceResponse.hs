{-# LANGUAGE OverloadedStrings #-}

module ProduceResponse where

import Data.Attoparsec.ByteString ((<?>), Parser)
import Data.Bifunctor
import Data.ByteString (ByteString)
import Data.Bytes.Types
import Data.Int
import Data.Map.Strict (Map)
import Data.Primitive
import Data.Text (Text)
import Data.Word
import GHC.Conc
import Socket.Stream.Interruptible.MutableBytes

import qualified Data.Attoparsec.ByteString as AT
import qualified Data.Map.Strict as Map

import Common

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

int16 :: Parser Int16
int16 = do
  a <- fromIntegral <$> AT.anyWord8
  b <- fromIntegral <$> AT.anyWord8
  pure (b + 0x100 * a)

int32 :: Parser Int32
int32 = do
  a <- fromIntegral <$> AT.anyWord8
  b <- fromIntegral <$> AT.anyWord8
  c <- fromIntegral <$> AT.anyWord8
  d <- fromIntegral <$> AT.anyWord8
  pure (d + 0x100 * c + 0x10000 * b + 0x1000000 * a)

int64 :: Parser Int64
int64 = do
  a <- fromIntegral <$> AT.anyWord8
  b <- fromIntegral <$> AT.anyWord8
  c <- fromIntegral <$> AT.anyWord8
  d <- fromIntegral <$> AT.anyWord8
  e <- fromIntegral <$> AT.anyWord8
  f <- fromIntegral <$> AT.anyWord8
  g <- fromIntegral <$> AT.anyWord8
  h <- fromIntegral <$> AT.anyWord8
  pure (h
    + 0x100 * g
    + 0x10000 * f
    + 0x1000000 * e
    + 0x100000000 * d
    + 0x10000000000 * c
    + 0x1000000000000 * b
    + 0x100000000000000 * a)

count :: Integral n => n -> Parser a -> Parser [a]
count = AT.count . fromIntegral

getProduceResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String ProduceResponse))
getProduceResponse kafka interrupt = do
  Right responseByteCount <- getResponseSizeHeader kafka interrupt
  responseBuffer <- newByteArray responseByteCount
  let responseBufferSlice = MutableBytes responseBuffer 0 responseByteCount
  responseStatus <- first toKafkaException <$>
    receiveExactly
      interrupt
      (getKafka kafka)
      responseBufferSlice
  responseBytes <- toByteString <$> unsafeFreezeByteArray responseBuffer
  pure $ AT.parseOnly parseProduceResponse responseBytes <$ responseStatus

getResponseSizeHeader ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException Int)
getResponseSizeHeader kafka interrupt = do
  responseSizeBuf <- newByteArray 4
  responseStatus <- first toKafkaException <$>
    receiveExactly
      interrupt
      (getKafka kafka)
      (MutableBytes responseSizeBuf 0 4)
  byteCount <- fromIntegral . byteSwap32 <$> readByteArray responseSizeBuf 0
  pure $ byteCount <$ responseStatus

errorCode :: Map Int Text
errorCode = Map.fromList
  [ (-1, "UNKNOWN_SERVER_ERROR")
  , (0, "NONE")
  , (1, "OFFSET_OUT_OF_RANGE")
  , (2, "CORRUPT_MESSAGE")
  , (3, "UNKNOWN_TOPIC_OR_PARTITION")
  , (4, "INVALID_FETCH_SIZE")
  , (5, "LEADER_NOT_AVAILABLE")
  ]
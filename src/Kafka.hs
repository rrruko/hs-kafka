{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka where

import Data.Attoparsec.ByteString ((<?>), Parser)
import Data.Bifunctor
import Data.Bits
import Data.ByteString (ByteString, unpack)
import Data.Digest.CRC32C
import Data.Foldable
import Data.Int
import Data.Word
import Data.IORef
import Data.Map.Strict (Map)
import Data.Primitive
import Data.Text (Text)
import Data.Text.Encoding
import GHC.Conc
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4
import System.Endian
--import Socket.Stream.Interruptible.MutableBytes

import qualified Data.Attoparsec.ByteString as AT
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

import Varint

newtype Kafka = Kafka { getKafka :: Connection }

data Topic = Topic
  ByteArray -- Topic name
  Int -- Number of partitions
  (IORef Int) -- incrementing number

data KafkaException = KafkaException String -- change later
  deriving Show

newKafka :: Endpoint -> IO (Either (ConnectException Uninterruptible) Kafka)
newKafka = fmap (fmap Kafka) . connect

produceApiVersion :: Word16
produceApiVersion = 7

produceApiKey :: Word16
produceApiKey = 0

produceRequestHeader :: ByteString -> ByteArray
produceRequestHeader name = fold
  [ byteArrayFromList [toBE16 produceApiKey]
  , byteArrayFromList [toBE16 produceApiVersion]
  , byteArrayFromList [toBE32 0]
  , byteArrayFromList [toBE16 $ fromIntegral $ BS.length name]
  , byteArrayFromList (unpack name)
  ]

produceRequestData ::
     Int -- Timeout
  -> Topic -- Topic
  -> ByteArray -- Payload
  -> ByteArray
produceRequestData timeout topic payload =
  let
    batchSize = size32 batch
    batch = recordBatch (mkRecord payload)
  in
    fold
      [ byteArrayFromList [toBE16 $ -1] -- transactional_id length
      , byteArrayFromList [toBE16 $ 1] -- acks
      , byteArrayFromList [toBE32 $ fromIntegral timeout] -- timeout in ms
      , byteArrayFromList [toBE32 $ 1] -- following array length
      , byteArrayFromList [toBE16 $ size16 topicName] -- following string length
      , topicName -- topic_data topic
      , byteArrayFromList [toBE32 $ 1] -- following array [data] length
      , byteArrayFromList [toBE32 $ 0] -- partition
      , byteArrayFromList [toBE32 $ batchSize] -- record_set length
      ]
      <> batch
  where
    Topic topicName _ _ = topic
    payloadSize = size32 payload
    topicNameSize = size32 topicName

size8 :: ByteArray -> Word8
size8 = fromIntegral . sizeofByteArray

size16 :: ByteArray -> Word16
size16 = fromIntegral . sizeofByteArray

size32 :: ByteArray -> Word32
size32 = fromIntegral . sizeofByteArray

mkRecord :: ByteArray -> ByteArray
mkRecord content =
  let
    recordLength = zigzag (sizeofByteArray recordBody)
    recordBody = fold
      [ byteArrayFromList [0 :: Word8] -- attributes
      , zigzag 0 -- timestampdelta varint
      , zigzag 0 -- offsetDelta varint
      , zigzag (-1) -- key length varint
      -- no key because key length is -1
      , zigzag (sizeofByteArray content) -- value length varint
      , content -- value
      , byteArrayFromList [0 :: Word8] --headers
      ]
  in
    recordLength <> recordBody
  where
    timestampDelta = zigzag 0
    offsetDelta = zigzag 0
    keyLength = zigzag (-1)
    valueLength = zigzag (sizeofByteArray content)

recordBatch :: ByteArray -> ByteArray
recordBatch records =
  let
    batchLength = 5 + size32 crc + size32 post
    pre = fold
      [ byteArrayFromList [toBE64 $ 0] -- baseOffset
      , byteArrayFromList [toBE32 $ batchLength] -- batchLength
      , byteArrayFromList [toBE32 $ 0] -- partitionLeaderEpoch
      , byteArrayFromList [2 :: Word8] -- magic
      ]
    crc = byteArrayFromList
      [ toBE32 . crc32c . BS.pack $ foldrByteArray (:) [] post
      ]
    post = fold
      [ byteArrayFromList [toBE16 $ 0] -- attributes
      , byteArrayFromList [toBE32 $ 0] -- lastOffsetDelta
      , byteArrayFromList [toBE64 $ 0] -- firstTimestamp
      , byteArrayFromList [toBE64 $ 0] -- lastTimestamp
      , byteArrayFromList [toBE64 $ -1] -- producerId
      , byteArrayFromList [toBE16 $ -1] -- producerEpoch
      , byteArrayFromList [toBE32 $ -1] -- baseSequence
      , byteArrayFromList [toBE32 $ 1] -- records array length
      ]
      <> records
  in
    pre <> crc <> post

testMessage ::
     Int
  -> Topic
  -> ByteArray
  -> IO ByteArray
testMessage timeout topic payload =
  let
    size = byteArrayFromList [toBE32 $ size32 $ msghdr <> msgdata]
    msghdr = produceRequestHeader "ruko"
    msgdata = produceRequestData 30000 topic payload
  in
    pure $ size <> msghdr <> msgdata

byteArrayFromByteString :: ByteString -> ByteArray
byteArrayFromByteString = byteArrayFromList . unpack

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

parseProduceResponse :: Parser ProduceResponse
parseProduceResponse = do
  lengthBytes <- int32 <?> "length bytes"
  correlationId <- int32 <?> "correlation id"
  responsesCount <- int32 <?> "responses count"
  produceResponseMessages <- count responsesCount parseProduceResponseMessage
    <?> "response messages"
  throttleTimeMs <- int32 <?> "throttle time"
  pure (ProduceResponse produceResponseMessages throttleTimeMs)

parseProduceResponseMessage :: Parser ProduceResponseMessage
parseProduceResponseMessage = do
  topicLengthBytes <- int16 <?> "topic length"
  topicName <- AT.take (fromIntegral topicLengthBytes) <?> "topic name"
  partitionResponseCount <- int32 <?> "partition response count"
  responses <- count partitionResponseCount parseProducePartitionResponse
    <?> "responses"
  pure (ProduceResponseMessage topicName responses)

count :: Integral n => n -> Parser a -> Parser [a]
count = AT.count . fromIntegral

parseProducePartitionResponse :: Parser ProducePartitionResponse
parseProducePartitionResponse = ProducePartitionResponse
  <$> int32
  <*> int16
  <*> int64
  <*> int64
  <*> int64

doStuff :: IO ()
doStuff = do
  kefka <- newKafka (Endpoint (IPv4 0) 9092)
  case kefka of
    Right k -> do
      a <- newIORef (0 :: Int)
      let topic = Topic (byteArrayFromByteString "test") 0 a
      msg <- testMessage 30000 topic (byteArrayFromByteString $
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" <>
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
      arr <- newUnliftedArray 1 msg
      farr <- freezeUnliftedArray arr 0 1
      v <- produce k topic 10000000 farr
      case v of
        Right (_, Right response) -> do
          print response
        Right (_, Left errorMsg) -> do
          print "Parsing failed"
          print errorMsg
        Left exception -> do
          print exception
    Left bad -> do
      print bad
      fail "Couldn't connect to kafka"

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException (Int, Either String ProduceResponse))
produce kafka topic waitTime payloads = do
  interrupt <- registerDelay waitTime
  arr <- newByteArray 100
  let barr = foldByteArrays payloads
  print barr
  interruptibleSendByteArray
    interrupt
    (getKafka kafka)
    barr
  mba <- newByteArray 100
  response <- first toKafkaException <$>
    interruptibleReceiveBoundedMutableByteArraySlice
      interrupt
      (getKafka kafka)
      1000
      mba
      0
  print =<< unsafeFreezeByteArray mba
  bytes <- BS.pack . foldrByteArray (:) [] <$> unsafeFreezeByteArray mba
  pure $ fmap (flip (,) (AT.parseOnly parseProduceResponse bytes)) response

toKafkaException :: ReceiveException 'Interruptible -> KafkaException
toKafkaException = KafkaException . show

foldByteArrays :: UnliftedArray ByteArray -> ByteArray
foldByteArrays = foldrUnliftedArray (<>) (byteArrayFromList ([]::[Char]))

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

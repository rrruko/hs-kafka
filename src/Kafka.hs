{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka where

import Data.Attoparsec.ByteString ((<?>), Parser)
import Data.Bifunctor
import Data.ByteString (ByteString, unpack)
import Data.Bytes.Types
import Data.Digest.CRC32C
import Data.Foldable
import Data.Int
import Data.IORef
import Data.Map.Strict (Map)
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Data.Text (Text)
import Data.Word
import GHC.Conc
import Net.IPv4 (IPv4(..))
import Socket.Stream.Interruptible.MutableBytes
import Socket.Stream.IPv4

import qualified Data.Attoparsec.ByteString as AT
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as Map

import Varint

newtype Kafka = Kafka { getKafka :: Connection }

data Topic = Topic
  ByteArray -- Topic name
  Int -- Number of partitions
  (IORef Int) -- incrementing number

data KafkaException = KafkaException String -- change later
  deriving Show

newKafka :: Peer -> IO (Either (ConnectException ('Internet 'V4) 'Uninterruptible) Kafka)
newKafka = fmap (fmap Kafka) . connect

produceApiVersion :: Int16
produceApiVersion = 7

produceApiKey :: Int16
produceApiKey = 0

toBE16 :: Int16 -> Int16
toBE16 = fromIntegral . byteSwap16 . fromIntegral 

toBE32 :: Int32 -> Int32
toBE32 = fromIntegral . byteSwap32 . fromIntegral 

toBEW32 :: Word32 -> Word32
toBEW32 = byteSwap32

toBE64 :: Int64 -> Int64
toBE64 = fromIntegral . byteSwap64 . fromIntegral 

produceRequestHeader :: ByteString -> ByteArray
produceRequestHeader name = fold
  [ byteArrayFromList [toBE16 produceApiKey]
  , byteArrayFromList [toBE16 produceApiVersion]
  , byteArrayFromList [toBE32 0xbeef]
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

size8 :: ByteArray -> Int8
size8 = fromIntegral . sizeofByteArray

size16 :: ByteArray -> Int16
size16 = fromIntegral . sizeofByteArray

size32 :: ByteArray -> Int32
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
      [ crc32c . BS.pack $ foldrByteArray (:) [] post
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
    msgdata = produceRequestData timeout topic payload
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
  kefka <- newKafka (Peer (IPv4 0) 9092)
  let thirtySecondsMs = 30000
      thirtySecondsUs = 30000000
  case kefka of
    Right k -> do
      partitionIndex <- newIORef (0 :: Int)
      let topic = Topic (byteArrayFromByteString "test") 0 partitionIndex
      msg <- testMessage thirtySecondsMs topic (byteArrayFromByteString $
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" <>
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
      arr <- newUnliftedArray 1 msg
      farr <- freezeUnliftedArray arr 0 1
      v <- produce k topic thirtySecondsUs farr
      case v of
        Right (Right response) -> do
          print response
        Right (Left errorMsg) -> do
          putStrLn "Parsing failed"
          print errorMsg
        Left exception -> do
          print exception
    Left bad -> do
      print bad
      fail "Couldn't connect to kafka"

getArray :: MutableBytes s -> MutableByteArray s
getArray (MutableBytes a _ _) = a

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException (Either String ProduceResponse))
produce kafka _ waitTime payloads = do
  interrupt <- registerDelay waitTime
  let msg = foldByteArrays payloads
      len = sizeofByteArray msg
  messageBuffer <- newByteArray len
  copyByteArray messageBuffer 0 msg 0 (sizeofByteArray msg)
  let messageBufferSlice = MutableBytes messageBuffer 0 len
  print =<< unsafeFreezeByteArray (getArray messageBufferSlice)
  _ <- send
    interrupt
    (getKafka kafka)
    messageBufferSlice
  responseSizeBuf <- newByteArray 4
  _ <- first toKafkaException <$>
    receiveExactly
      interrupt
      (getKafka kafka)
      (MutableBytes responseSizeBuf 0 4)

  responseByteCount <- fromIntegral . byteSwap32 <$> readByteArray responseSizeBuf 0
  putStrLn $ "Expecting "  <> show responseByteCount <> " bytes from kafka"
  print =<< unsafeFreezeByteArray responseSizeBuf
  responseBuffer <- newByteArray responseByteCount
  let responseBufferSlice = MutableBytes responseBuffer 0 responseByteCount
  responseStatus <- first toKafkaException <$>
    receiveExactly
      interrupt
      (getKafka kafka)
      responseBufferSlice
  responseBytes <- BS.pack . foldrByteArray (:) [] <$>
    unsafeFreezeByteArray responseBuffer
  print =<< unsafeFreezeByteArray responseBuffer
  pure $
    fmap
      (const (AT.parseOnly parseProduceResponse responseBytes))
      responseStatus

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

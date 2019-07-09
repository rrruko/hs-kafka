{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
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
  -> UnliftedArray ByteArray -- Payload
  -> ByteArray
produceRequestData timeout topic payloads =
  let
    batchSize = size32 batch
    batch = recordBatch $ mkRecordBatch payloads
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

mkRecordBatch :: UnliftedArray ByteArray -> UnliftedArray ByteArray
mkRecordBatch payloads =
  unliftedArrayFromList $
    zipWith mkRecord (unliftedArrayToList payloads) [0..]

size8 :: ByteArray -> Int8
size8 = fromIntegral . sizeofByteArray

size16 :: ByteArray -> Int16
size16 = fromIntegral . sizeofByteArray

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

mkRecord :: ByteArray -> Int -> ByteArray
mkRecord content index =
  let
    recordLength = zigzag (sizeofByteArray recordBody)
    recordBody = fold
      [ byteArrayFromList [0 :: Word8] -- attributes
      , zigzag 0 -- timestampdelta varint
      , zigzag index -- offsetDelta varint
      , zigzag (-1) -- key length varint
      -- no key because key length is -1
      , zigzag (sizeofByteArray content) -- value length varint
      , content -- value
      , byteArrayFromList [0 :: Word8] --headers
      ]
  in
    recordLength <> recordBody

recordBatch :: UnliftedArray ByteArray -> ByteArray
recordBatch records =
  let
    recordsCount = fromIntegral $ sizeofUnliftedArray records
    batchLength = 5 + size32 crc + size32 post
    pre = fold
      [ byteArrayFromList [toBE64 $ 0] -- baseOffset
      , byteArrayFromList [toBE32 $ batchLength] -- batchLength
      , byteArrayFromList [toBE32 $ 0] -- partitionLeaderEpoch
      , byteArrayFromList [2 :: Word8] -- magic
      ]
    crc = byteArrayFromList
      [ toBEW32 . crc32c . toByteString $ post
      ]
    post = fold
      [ byteArrayFromList [toBE16 $ 0] -- attributes
      , byteArrayFromList [toBE32 $ recordsCount - 1] -- lastOffsetDelta
      , byteArrayFromList [toBE64 $ 0] -- firstTimestamp
      , byteArrayFromList [toBE64 $ 0] -- lastTimestamp
      , byteArrayFromList [toBE64 $ -1] -- producerId
      , byteArrayFromList [toBE16 $ -1] -- producerEpoch
      , byteArrayFromList [toBE32 $ -1] -- baseSequence
      , byteArrayFromList [toBE32 $ recordsCount] -- records array length
      ]
      <> foldrUnliftedArray (<>) mempty records
  in
    pre <> crc <> post

produceRequest ::
     Int
  -> Topic
  -> UnliftedArray ByteArray
  -> ByteArray
produceRequest timeout topic payloads =
  let
    size = byteArrayFromList [toBE32 $ size32 $ msghdr <> msgdata]
    msghdr = produceRequestHeader "ruko"
    msgdata = produceRequestData timeout topic payloads
  in
    size <> msghdr <> msgdata

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
  let thirtySecondsUs = 30000000
  newKafka (Peer (IPv4 0) 9092) >>= \case
    Right kafka -> do
      partitionIndex <- newIORef (0 :: Int)
      let topic = Topic (byteArrayFromByteString "test") 0 partitionIndex
      let msg = unliftedArrayFromList
            [ fromByteString "aaaaa"
            , fromByteString "bbbbb"
            , fromByteString "ccccc"
            ]
      v <- produce kafka topic thirtySecondsUs msg
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
produce kafka topic waitTime payloads = do
  interrupt <- registerDelay waitTime
  let message = produceRequest (waitTime `div` 1000) topic payloads
  print message
  _ <- sendProduceRequest kafka interrupt message
  getProduceResponse kafka interrupt

produce' :: UnliftedArray ByteArray -> ByteString -> IO ()
produce' bytes topicName = do
  topic <- Topic (fromByteString topicName) 0 <$> newIORef 0
  Right k <- newKafka (Peer (IPv4 0) 9092)
  _ <- produce k topic 30000000 bytes
  pure ()

sendProduceRequest ::
     Kafka
  -> TVar Bool
  -> ByteArray
  -> IO (Either (SendException 'Interruptible) ())
sendProduceRequest kafka interrupt message = do
  let len = sizeofByteArray message
  messageBuffer <- newByteArray len
  copyByteArray messageBuffer 0 message 0 (sizeofByteArray message)
  let messageBufferSlice = MutableBytes messageBuffer 0 len
  send
    interrupt
    (getKafka kafka)
    messageBufferSlice

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
  print =<< unsafeFreezeByteArray responseBuffer
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

toByteString :: ByteArray -> ByteString
toByteString = BS.pack . foldrByteArray (:) []

fromByteString :: ByteString -> ByteArray
fromByteString = byteArrayFromList . BS.unpack

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

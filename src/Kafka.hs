{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka where

import Data.Bifunctor
import Data.Bits
import Data.ByteString
import Data.Foldable
import Data.Int
import Data.Word
import Data.IORef
import Data.Primitive
import Data.Text.Encoding
import GHC.Conc
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4 
import System.Endian
--import Socket.Stream.Interruptible.MutableBytes

newtype Kafka = Kafka { getKafka :: Connection }

data Topic = Topic
  ByteArray -- Topic name
  Int -- Number of partitions
  (IORef Int) -- incrementing number

data KafkaException = KafkaException String -- change later
  deriving Show

newKafka :: Endpoint -> IO (Either (ConnectException Uninterruptible) Kafka)
newKafka = fmap (fmap Kafka) . connect

  {-
Produce Request (Version: 7) => transactional_id acks timeout [topic_data]
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout => INT32
  topic_data => topic [data]
    topic => STRING
    data => partition record_set
      partition => INT32
      record_set => RECORDS
  -}

testMessage :: IO ByteArray
testMessage =
  let
    size =
      [ byteArrayFromList [toBE32 $ len $ fold (msghdr <> msgdata)]
      ]
    msghdr =
      [ byteArrayFromList [toBE16 $ 0] -- produce api key 0
      , byteArrayFromList [toBE16 $ 7] -- api_version
      , byteArrayFromList [toBE32 $ 0] -- correlation_id
      , byteArrayFromList [toBE16 $ -1] -- client_id nullable_string length
      ]
    msgdata =
      [ byteArrayFromList [toBE16 $ -1] -- transactional_id length
      , byteArrayFromList [toBE16 $ 0] -- acks
      , byteArrayFromList [toBE32 $ 1000] -- timeout in ms
      , byteArrayFromList [toBE32 $ 1] -- following array length
      , byteArrayFromList [toBE16 $ 4] -- following string length
      , byteArrayFromList (unpack $ encodeUtf8 $ "test") -- topic_data topic
      , byteArrayFromList [toBE32 $ 1] -- following array length
      , byteArrayFromList [toBE32 $ 0] -- partition
      , byteArrayFromList [toBE32 $ 123] -- record set length
      ]
  in 
    pure $ fold $ size <> msghdr <> msgdata
  where
  len = foldrByteArray (\(e::Word8) a -> a + 1) 0

doStuff :: IO (Either KafkaException ())
doStuff = do
  kefka <- newKafka (Endpoint (IPv4 0) 9092)
  case kefka of
    Right k -> do
      a <- newIORef (0 :: Int)
      msg <- testMessage
      let topic = Topic (byteArrayFromList "topic") 0 a
      arr <- newUnliftedArray 1 msg
      farr <- freezeUnliftedArray arr 0 1
      produce k topic 10000000 farr
    Left bad -> print bad *> pure (Right ())

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException ())
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
      100
      mba
      0
  pure (second (const ()) response)

toKafkaException :: ReceiveException 'Interruptible -> KafkaException
toKafkaException = KafkaException . show

foldByteArrays :: UnliftedArray ByteArray -> ByteArray
foldByteArrays = foldrUnliftedArray (<>) (byteArrayFromList ([]::[Char]))

{-
  send
    interrupt
    (getKafka kafka)
    (foldrUnliftedArray (\ba acc -> acc) _ payloads)
  bytesReceived <- 
    receiveOnce
      interrupt 
      (getKafka kafka)
      1024
  print bytesReceived
-}

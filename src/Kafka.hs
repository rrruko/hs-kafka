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
      , byteArrayFromList [toBE16 $ 4] -- client_id nullable_string length
      , byteArrayFromList (unpack $ encodeUtf8 $ "ruko")
      ]
    msgdata =
      [ byteArrayFromList [toBE16 $ -1] -- transactional_id length
      , byteArrayFromList [toBE16 $ 1] -- acks
      , byteArrayFromList [toBE32 $ 30000] -- timeout in ms
      , byteArrayFromList [toBE32 $ 1] -- following array length
      , byteArrayFromList [toBE16 $ 4] -- following string length
      , byteArrayFromList (unpack $ encodeUtf8 $ "test") -- topic_data topic
      , byteArrayFromList [toBE32 $ 1] -- following array [data] length
      , byteArrayFromList [toBE32 $ 0] -- partition
      , byteArrayFromList [toBE32 $ 0x48] -- record_set length
      , recordBatch (mkRecord (unpack $ encodeUtf8 $ "awoo"))
      ]
    mkRecord content = fold
      [ byteArrayFromList [0x14 :: Word8] -- length
      , byteArrayFromList [0 :: Word8] -- attributes
      , byteArrayFromList [0 :: Word8] -- timestampdelta varint
      , byteArrayFromList [0 :: Word8] -- offsetDelta varint
      , byteArrayFromList [1 :: Word8] -- key length varint
      -- no key because key length is -1
      , byteArrayFromList [8 :: Word8] -- value length varint
      , byteArrayFromList content -- value
      , byteArrayFromList [0 :: Word8] --headers
      ]
    recordBatch records = fold
      [ byteArrayFromList [toBE64 $ 0] -- baseOffset
      , byteArrayFromList [toBE32 $ 0x3c] -- batchLength
      , byteArrayFromList [toBE32 $ 0] -- partitionLeaderEpoch
      , byteArrayFromList [2 :: Word8] -- magic
      , byteArrayFromList [toBE32 $ 0] -- crc
      , byteArrayFromList [toBE16 $ 0] -- attributes
      , byteArrayFromList [toBE32 $ 0] -- lastOffsetDelta
      , byteArrayFromList [toBE64 $ 0] -- firstTimestamp
      , byteArrayFromList [toBE64 $ -1] -- lastTimestamp
      , byteArrayFromList [toBE64 $ -1] -- producerId
      , byteArrayFromList [toBE16 $ -1] -- producerEpoch
      , byteArrayFromList [toBE32 $ -1] -- baseSequence
      , byteArrayFromList [toBE32 $ 1] -- records array length
      ]
      <> records
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
      v <- produce k topic 10000000 farr
      print v
      pure $ second (const ()) v
    Left bad -> print bad *> pure (Right ())

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException Int)
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
  print =<< unsafeFreezeByteArray mba
  pure response

toKafkaException :: ReceiveException 'Interruptible -> KafkaException
toKafkaException = KafkaException . show

foldByteArrays :: UnliftedArray ByteArray -> ByteArray
foldByteArrays = foldrUnliftedArray (<>) (byteArrayFromList ([]::[Char]))

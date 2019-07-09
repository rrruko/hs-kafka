{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ProduceRequest where

import Data.ByteString (ByteString, unpack)
import Data.Bytes.Types
import Data.Digest.CRC32C
import Data.Foldable
import Data.Int
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Data.Word
import GHC.Conc
import Socket.Stream.Interruptible.MutableBytes
import Socket.Stream.IPv4

import qualified Data.ByteString as BS

import Common
import Varint

produceApiVersion :: Int16
produceApiVersion = 7

produceApiKey :: Int16
produceApiKey = 0

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

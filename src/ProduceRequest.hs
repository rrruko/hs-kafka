{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module ProduceRequest where

import Control.Monad.ST
import Data.ByteString (ByteString)
import Data.Bytes.Types
import Data.Digest.CRC32C
import Data.Foldable
import Data.Int
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned
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

clientId :: ByteString
clientId = "ruko"

correlationId :: Int32
correlationId = 0xbeef

produceRequestHeader :: ByteString -> ByteArray
produceRequestHeader name = runST $ do
  arr <- newByteArray 10
  writeUnalignedByteArray arr 0 (toBE16 produceApiKey)
  writeUnalignedByteArray arr 2 (toBE16 produceApiVersion)
  writeUnalignedByteArray arr 4 (toBE32 correlationId)
  writeUnalignedByteArray arr 8 (toBE16 (fromIntegral (BS.length name)))
  unsafeFreezeByteArray arr <> pure (fromByteString name)

produceRequestData ::
     Int -- Timeout
  -> Topic -- Topic
  -> UnliftedArray ByteArray -- Payload
  -> ByteArray
produceRequestData timeout topic payloads =
  let
    batchSize = size32 batch
    batch = recordBatch $ mkRecordBatch payloads
    topicNameSize = sizeofByteArray topicName
    prefix = runST $ do
      arr <- newByteArray (26 + topicNameSize)
      writeUnalignedByteArray arr 0 (toBE16 (-1)) -- transactional_id length
      writeUnalignedByteArray arr 2 (toBE16 1) -- acks
      writeUnalignedByteArray arr 4 (toBE32 $ fromIntegral timeout) -- timeout in ms
      writeUnalignedByteArray arr 8 (toBE32 1) -- following array length
      writeUnalignedByteArray arr 12 (toBE16 $ size16 topicName) -- following string length
      copyByteArray arr 14 topicName 0 (topicNameSize) -- topic_data topic
      writeUnalignedByteArray arr (14 + topicNameSize) (toBE32 1) -- following array [data] length
      writeUnalignedByteArray arr (18 + topicNameSize) (toBE32 0) -- partition
      writeUnalignedByteArray arr (22 + topicNameSize) (toBE32 batchSize) -- record_set length
      unsafeFreezeByteArray arr
    in
      prefix <> batch
  where
    Topic topicName _ _ = topic

mkRecordBatch :: UnliftedArray ByteArray -> UnliftedArray ByteArray
mkRecordBatch payloads = runUnliftedArray $ do
  arr <- newUnliftedArray (sizeofUnliftedArray payloads) mempty
  itraverseUnliftedArray_
    (\i ba ->
      writeUnliftedArray arr i (mkRecord ba i))
    payloads
  pure arr

mkRecord :: ByteArray -> Int -> ByteArray
mkRecord content index =
  let
    recordLength = zigzag (sizeofByteArray recordBody)
    dynamicContent = fold
      [ zigzag 0
      , zigzag index
      , zigzag (-1)
      , zigzag (sizeofByteArray content)
      , content
      ]
    dynamicContentSize = sizeofByteArray dynamicContent
    recordBody = runST $ do
      arr <- newByteArray (2 + dynamicContentSize)
      writeUnalignedByteArray arr 0 (0 :: Word8) -- attributes
      copyByteArray arr 1 dynamicContent 0 dynamicContentSize
      writeUnalignedByteArray arr (1 + dynamicContentSize) (0 :: Word8) --headers
      unsafeFreezeByteArray arr
  in
    recordLength <> recordBody

recordBatch :: UnliftedArray ByteArray -> ByteArray
recordBatch records =
  let
    recordsCount = fromIntegral $ sizeofUnliftedArray records
    batchLength = 5 + size32 crc + size32 post
    pre = runST $ do
      arr <- newByteArray 17
      writeUnalignedByteArray arr 0 (toBE64 0) -- baseOffset
      writeUnalignedByteArray arr 8 (toBE32 batchLength) -- batchLength
      writeUnalignedByteArray arr 12 (toBE32 0) -- partitionLeaderEpoch
      writeUnalignedByteArray arr 16 (2 :: Word8) -- magic
      unsafeFreezeByteArray arr
    crc = byteArrayFromList
      [ toBEW32 . crc32c . toByteString $ post
      ]
    post' = runST $ do
      arr <- newByteArray 40
      writeUnalignedByteArray arr 0 (toBE16 0) -- attributes
      writeUnalignedByteArray arr 2 (toBE32 $ recordsCount - 1) -- lastOffsetDelta
      writeUnalignedByteArray arr 6 (toBE64 0) -- firstTimestamp
      writeUnalignedByteArray arr 14 (toBE64 0) -- lastTimestamp
      writeUnalignedByteArray arr 22 (toBE64 (-1)) -- producerId
      writeUnalignedByteArray arr 30 (toBE16 (-1)) -- producerEpoch
      writeUnalignedByteArray arr 32 (toBE32 (-1)) -- baseSequence
      writeUnalignedByteArray arr 36 (toBE32 recordsCount) -- records array length
      unsafeFreezeByteArray arr
    post = post' <> foldrUnliftedArray (<>) mempty records
  in
    pre <> crc <> post

produceRequest ::
     Int
  -> Topic
  -> UnliftedArray ByteArray
  -> ByteArray
produceRequest timeout topic payloads =
  let
    msghdr = produceRequestHeader clientId
    msgdata = produceRequestData timeout topic payloads
    hdrSize = sizeofByteArray msghdr
    dataSize = sizeofByteArray msgdata
  in
    runST $ do
      buf <- newByteArray (4 + hdrSize + dataSize)
      writeUnalignedByteArray buf 0 (toBE32 $ fromIntegral $ hdrSize + dataSize)
      copyByteArray buf 4 msghdr 0 hdrSize
      copyByteArray buf (4 + hdrSize) msgdata 0 dataSize
      unsafeFreezeByteArray buf

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

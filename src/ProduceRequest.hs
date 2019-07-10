{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module ProduceRequest where

import Control.Monad.ST
import Data.ByteString (ByteString)
import Data.Bytes.Types
import Data.Foldable
import Data.Int
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned
import Data.Primitive.Slice (UnliftedVector(UnliftedVector))
import Data.Word
import GHC.Conc
import Socket.Stream.Interruptible.MutableBytes
import Socket.Stream.IPv4

import qualified Crc32c as CRC
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

magic :: Word8
magic = 2

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
  -> Int -- Size of record batch
  -> ByteArray
produceRequestData timeout topic batchSize =
  let
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
      writeUnalignedByteArray arr (22 + topicNameSize) (toBE32 $ fromIntegral batchSize) -- record_set length
      unsafeFreezeByteArray arr
    in
      prefix
  where
    Topic topicName _ _ = topic

imapUnliftedArray :: 
     (Int -> ByteArray -> ByteArray)
  -> UnliftedArray ByteArray
  -> UnliftedArray ByteArray
imapUnliftedArray f a = runUnliftedArray $ do
  arr <- newUnliftedArray (sizeofUnliftedArray a) mempty
  itraverseUnliftedArray_
    (\i element ->
      writeUnliftedArray arr i (f i element))
    a 
  pure arr

makeRecordMetadata :: Int -> ByteArray -> ByteArray
makeRecordMetadata index content =
  let
    -- plus one is for the trailing null byte
    recordLength = zigzag (sizeofByteArray metadataContent + sizeofByteArray content + 1)
    metadataContent = fold
      [ byteArrayFromList [0 :: Word8]
      , zigzag 0 -- timestampDelta
      , zigzag index -- offsetDelta
      , zigzag (-1) -- keyLength
      , zigzag (sizeofByteArray content) -- valueLen
      ]
    metadataContentSize = sizeofByteArray metadataContent
  in
    recordLength <> metadataContent

sumSizes :: UnliftedArray ByteArray -> Int
sumSizes = foldrUnliftedArray (\e acc -> acc + sizeofByteArray e) 0

produceRequestRecordBatchMetadata ::
     UnliftedArray ByteArray
  -> Int
  -> Int
  -> ByteArray
produceRequestRecordBatchMetadata payloadsSectionChunks payloadCount payloadsSectionSize = 
  let
    crc =
      CRC.chunks
        (CRC.bytes 0 (Bytes postCrc 0 40))
        (UnliftedVector payloadsSectionChunks 0 (3*payloadCount))
    batchLength = 9 + 40 + fromIntegral payloadsSectionSize
    preCrc = runST $ do
      arr <- newByteArray 21
      writeUnalignedByteArray arr 0 (toBE64 0)
      writeUnalignedByteArray arr 8 (toBE32 batchLength)
      writeUnalignedByteArray arr 12 (toBE32 0)
      writeUnalignedByteArray arr 16 magic
      writeUnalignedByteArray arr 17 (toBE32 $ fromIntegral $ crc)
      unsafeFreezeByteArray arr
    postCrc = runST $ do
      arr <- newByteArray 40
      writeUnalignedByteArray arr 0 (toBE16 0)
      writeUnalignedByteArray arr 2 (toBE32 $ fromIntegral $ payloadCount - 1)
      writeUnalignedByteArray arr 6 (toBE64 0)
      writeUnalignedByteArray arr 14 (toBE64 0)
      writeUnalignedByteArray arr 22 (toBE64 (-1))
      writeUnalignedByteArray arr 30 (toBE16 (-1))
      writeUnalignedByteArray arr 32 (toBE32 (-1))
      writeUnalignedByteArray arr 36 (toBE32 $ fromIntegral payloadCount)
      unsafeFreezeByteArray arr
  in
    preCrc <> postCrc

makeRequestMetadata :: 
     Int
  -> Int
  -> Topic 
  -> ByteArray
makeRequestMetadata recordBatchSectionSize timeout topic =
     produceRequestHeader clientId
  <> produceRequestData timeout topic recordBatchSectionSize 

produceRequest ::
     Int
  -> Topic
  -> UnliftedArray ByteArray
  -> UnliftedArray ByteArray
produceRequest timeout topic payloads =
  let
    payloadCount = sizeofUnliftedArray payloads
    zero = runST $ do
      ba <- newByteArray 1
      writeByteArray ba 0 (0 :: Word8)
      unsafeFreezeByteArray ba
    payloadMetadatas = imapUnliftedArray makeRecordMetadata payloads
    recordBatchSectionSize =
        sumSizes payloadsSectionChunks 
      + sizeofByteArray recordBatchMetadata
    requestMetadata = makeRequestMetadata
      recordBatchSectionSize
      timeout
      topic
    recordBatchMetadata =
      produceRequestRecordBatchMetadata
        payloadsSectionChunks
        payloadCount
        (sumSizes payloadsSectionChunks)
    payloadsSectionChunks = runUnliftedArray $ do
      arr <- newUnliftedArray (3 * payloadCount) zero
      itraverseUnliftedArray_
        (\i payload -> do
          let payloadMeta = indexUnliftedArray payloadMetadatas i
          writeUnliftedArray arr (i * 3)     payloadMeta
          writeUnliftedArray arr (i * 3 + 1) payload
          writeUnliftedArray arr (i * 3 + 2) zero)
        payloads
      pure arr
    totalRequestSizeHeader = 
      byteArrayFromList
        [ 0 :: Word8
        , 0
        , 0
        , fromIntegral $
              sizeofByteArray requestMetadata
            + sizeofByteArray recordBatchMetadata
            + sumSizes payloadsSectionChunks
        ]
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray (3 * payloadCount + 3) zero
      writeUnliftedArray arr 0 totalRequestSizeHeader
      writeUnliftedArray arr 1 requestMetadata
      writeUnliftedArray arr 2 recordBatchMetadata
      copyUnliftedArray arr 3 payloadsSectionChunks 0 (3 * payloadCount)
      pure arr

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

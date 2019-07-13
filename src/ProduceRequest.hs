{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnboxedTuples #-}

module ProduceRequest
  ( produceRequest
  ) where

import Control.Monad.ST
import Data.Bytes.Types
import Data.Foldable
import Data.Int
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Data.Primitive.Slice (UnliftedVector(UnliftedVector))
import Data.Word

import qualified Crc32c as CRC

import Common
import KafkaWriter
import Varint

produceApiVersion :: Int16
produceApiVersion = 7

produceApiKey :: Int16
produceApiKey = 0

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
    preCrc = evaluate $ foldBuilder
      [ build64 0
      , build32 batchLength
      , build32 0
      , build8 magic
      , build32 (fromIntegral crc)
      ]
    postCrc = evaluate $ foldBuilder
      [ build16 0
      , build32 (fromIntegral (payloadCount - 1))
      , build64 0
      , build64 0
      , build64 (-1)
      , build16 (-1)
      , build32 (-1)
      , build32 $ fromIntegral payloadCount
      ]
  in
    preCrc <> postCrc

makeRequestMetadata ::
     Int
  -> Int
  -> Topic
  -> ByteArray
makeRequestMetadata recordBatchSectionSize timeout topic =
  evaluate $ foldBuilder 
    [ build32 (fromIntegral $ 36 + clientIdLength + topicNameSize + recordBatchSectionSize)
    , build16 produceApiKey
    , build16 produceApiVersion
    , build32 correlationId
    , build16 (fromIntegral clientIdLength)
    , buildArray (fromByteString clientId) clientIdLength
    , build16 (-1) -- transactional_id length
    , build16 1 -- acks
    , build32 (fromIntegral timeout) -- timeout in ms
    , build32 1 -- following array length
    , build16 (size16 topicName) -- following string length
    , buildArray topicName topicNameSize -- topic_data topic
    , build32 1 -- following array [data] length
    , build32 0 -- partition
    , build32 (fromIntegral recordBatchSectionSize) -- record_set length
    ]
  where
    Topic topicName _ _ = topic
    topicNameSize = sizeofByteArray topicName

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
          writeUnliftedArray arr (i * 3)     (makeRecordMetadata i payload)
          writeUnliftedArray arr (i * 3 + 1) payload
          writeUnliftedArray arr (i * 3 + 2) zero)
        payloads
      pure arr
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray (3 * payloadCount + 2) zero
      writeUnliftedArray arr 0 requestMetadata
      writeUnliftedArray arr 1 recordBatchMetadata
      copyUnliftedArray arr 2 payloadsSectionChunks 0 (3 * payloadCount)
      pure arr

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE UndecidableInstances #-}

module Kafka.Internal.Produce.Request
  ( produceRequest
  ) where

import Control.Monad.ST
import Data.Bytes.Types
import Data.Foldable
import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Slice (UnliftedVector(UnliftedVector))
import Data.Primitive.Unlifted.Array
import Data.Word

import qualified Crc32c as CRC

import Kafka.Common
import Kafka.Internal.Writer
import Kafka.Internal.Zigzag (zigzag)

produceApiVersion :: Int16
produceApiVersion = 7

produceApiKey :: Int16
produceApiKey = 0

defaultBaseOffset :: Int64
defaultBaseOffset = 0

defaultPartitionLeaderEpoch :: Int32
defaultPartitionLeaderEpoch = 0

defaultRecordAttributes :: Int8
defaultRecordAttributes = 0

defaultTimestampDelta :: Int
defaultTimestampDelta = 0

defaultFirstTimestamp :: Int64
defaultFirstTimestamp = 0

defaultMaxTimestamp :: Int64
defaultMaxTimestamp = 0

defaultProducerId :: Int64
defaultProducerId = -1

defaultProducerEpoch :: Int16
defaultProducerEpoch = -1

defaultBaseSequence :: Int32
defaultBaseSequence = -1

defaultRecordBatchAttributes :: Int16
defaultRecordBatchAttributes = 0

data Acknowledgments
  = AckLeaderOnly
  | NoAcknowledgments
  | AckFullISR

defaultAcknowledgments :: Acknowledgments
defaultAcknowledgments = AckLeaderOnly

acks :: Acknowledgments -> Int16
acks AckLeaderOnly = 1
acks NoAcknowledgments = 0
acks AckFullISR = -1

makeRecordMetadata :: Int -> ByteArray -> ByteArray
makeRecordMetadata index content =
  let
    -- plus one is for the trailing null byte
    recordLength = zigzag (sizeofByteArray metadataContent + sizeofByteArray content + 1)
    metadataContent = fold
      [ byteArrayFromList [defaultRecordAttributes]
      , zigzag defaultTimestampDelta
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
        (CRC.bytes 0 (Bytes postCrc 0 postCrcLength))
        (UnliftedVector payloadsSectionChunks 0 (3*payloadCount))
    batchLength = fromIntegral $
        preCrcLength
      + postCrcLength
      + payloadsSectionSize
    preCrcLength = 9
    preCrc = build $
      int64 defaultBaseOffset
      <> int32 batchLength
      <> int32 defaultPartitionLeaderEpoch
      <> int8 magic
      <> int32 (fromIntegral crc)
    postCrcLength = 40
    postCrc = build $
      int16 defaultRecordBatchAttributes
      <> int32 (fromIntegral (payloadCount - 1))
      <> int64 defaultFirstTimestamp
      <> int64 defaultMaxTimestamp
      <> int64 defaultProducerId
      <> int16 defaultProducerEpoch
      <> int32 defaultBaseSequence
      <> int32 (fromIntegral payloadCount)
  in
    preCrc <> postCrc

makeRequestMetadata ::
     Int
  -> Int
  -> TopicName
  -> Int32
  -> ByteArray
makeRequestMetadata recordBatchSectionSize timeout topic partition =
  build $
    int32 size
    <> int16 produceApiKey
    <> int16 produceApiVersion
    <> int32 correlationId
    <> string (fromByteString clientId) clientIdLength
    <> int16 (-1) -- transactional_id length
    <> int16 (acks defaultAcknowledgments) -- acks
    <> int32 (fromIntegral timeout) -- timeout in ms
    <> int32 1 -- following array length
    <> string topicName topicNameSize -- topic_data topic
    <> int32 1 -- following array [data] length
    <> int32 partition -- partition
    <> int32 (fromIntegral recordBatchSectionSize) -- record_set length
  where
    TopicName topicName = topic
    topicNameSize = sizeofByteArray topicName
    minimumSize = 36
    size = fromIntegral $
        minimumSize
      + clientIdLength
      + topicNameSize
      + recordBatchSectionSize

produceRequest ::
     Int
  -> TopicName
  -> Int32
  -> UnliftedArray ByteArray
  -> UnliftedArray ByteArray
produceRequest timeout topic partition payloads =
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
      partition
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

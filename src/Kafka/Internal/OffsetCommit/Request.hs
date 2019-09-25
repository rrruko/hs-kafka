module Kafka.Internal.OffsetCommit.Request
  ( offsetCommitRequest
  ) where

import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

import qualified String.Ascii as S

serializePartition :: PartitionOffset -> Builder
serializePartition a =
  int32 (partitionIndex a)
  <> int64 (partitionOffset a)
  <> int32 (-1) -- leader epoch
  <> int16 (-1) -- metadata

offsetCommitApiKey :: Int16
offsetCommitApiKey = 8

offsetCommitApiVersion :: Int16
offsetCommitApiVersion = 6

offsetCommitRequest ::
     TopicName
  -> [PartitionOffset]
  -> GroupMember
  -> GenerationId
  -> UnliftedArray ByteArray
offsetCommitRequest topic offs groupMember generationId =
  let
    GroupMember (GroupName gid) mid = groupMember
    GenerationId genId = generationId
    reqSize = build (int32 (size32 req))
    req =
      build $
        int16 offsetCommitApiKey
        <> int16 offsetCommitApiVersion
        <> int32 correlationId
        <> string clientId clientIdLength
        <> string gid (S.length gid)
        <> int32 genId
        <> maybe
            (int16 0)
            (\m -> bytearray m (sizeofByteArray m))
            mid
        <> int32 1 -- 1 topic
        <> topicName topic
        <> int32 (fromIntegral $ length offs)
        <> foldl'
            (\acc e -> acc <> serializePartition e)
            mempty
            offs
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

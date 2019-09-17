module Kafka.Internal.OffsetCommit.Request
  ( offsetCommitRequest
  ) where

import Data.Int
import Data.List
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

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
    GroupMember gid mid = groupMember
    TopicName topicName = topic
    GenerationId genId = generationId
    reqSize = build (int32 (size32 req))
    req =
      build $
        int16 offsetCommitApiKey
        <> int16 offsetCommitApiVersion
        <> int32 correlationId
        <> string (fromByteString clientId) clientIdLength
        <> string gid (sizeofByteArray gid)
        <> int32 genId
        <> maybe
            (int16 0)
            (\m -> string m (sizeofByteArray m))
            mid
        <> int32 1 -- 1 topic
        <> string topicName (sizeofByteArray topicName)
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

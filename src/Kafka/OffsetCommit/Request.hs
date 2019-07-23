module Kafka.OffsetCommit.Request
  ( offsetCommitRequest
  ) where

import Data.Int
import Data.List
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Writer

serializePartition :: PartitionOffset -> KafkaWriterBuilder s
serializePartition a = foldBuilder
  [ build32 (partitionIndex a)
  , build64 (partitionOffset a)
  , build32 (-1) -- leader epoch
  , build32 (-1) -- metadata
  ]

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
    reqSize = evaluate $ foldBuilder [build32 (size32 req)]
    req =
      evaluate $ foldBuilder
        [ build16 offsetCommitApiKey
        , build16 offsetCommitApiVersion
        , build32 correlationId
        , buildString gid (sizeofByteArray gid)
        , build32 genId
        , maybe
            (build16 0)
            (\m -> buildString m (sizeofByteArray m))
            mid
        , build32 1 -- 1 topic
        , buildString topicName (sizeofByteArray topicName)
        , build32 (fromIntegral $ length offs)
        , foldl'
            (\acc e -> acc <> serializePartition e)
            mempty
            offs
        ]
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

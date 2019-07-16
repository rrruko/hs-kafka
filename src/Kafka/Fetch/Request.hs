module Kafka.Fetch.Request
  ( fetchRequest
  , sessionlessFetchRequest
  ) where

import Data.Foldable (foldl')
import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Writer

fetchApiVersion :: Int16
fetchApiVersion = 10

fetchApiKey :: Int16
fetchApiKey = 1

data IsolationLevel
  = ReadUncommitted
  | ReadCommitted

isolationLevel :: IsolationLevel -> Int8
isolationLevel ReadUncommitted = 0
isolationLevel ReadCommitted = 1

sessionlessFetchRequest ::
     Int
  -> Topic
  -> [Partition]
  -> UnliftedArray ByteArray
sessionlessFetchRequest = fetchRequest 0 (-1)

defaultReplicaId :: Int32
defaultReplicaId = -1

defaultMinBytes :: Int32
defaultMinBytes = 1

defaultMaxBytes :: Int32
defaultMaxBytes = 30 * 1000 * 1000

defaultCurrentLeaderEpoch :: Int32
defaultCurrentLeaderEpoch = -1

defaultLogStartOffset :: Int64
defaultLogStartOffset = -1

fetchRequest ::
     Int32
  -> Int32
  -> Int
  -> Topic
  -> [Partition]
  -> UnliftedArray ByteArray
fetchRequest fetchSessionId fetchSessionEpoch timeout topic partitions =
  let
    minimumRequestSize = 49
    partitionMessageSize = 28
    requestSize = minimumRequestSize
      + partitionMessageSize * partitionCount
      + topicNameSize
      + clientIdLength
    requestMetadata = evaluate $ foldBuilder $
      [ build32 (fromIntegral requestSize) -- size
      -- common request headers
      , build16 fetchApiKey
      , build16 fetchApiVersion
      , build32 correlationId
      , build16 (fromIntegral clientIdLength)
      , buildArray (fromByteString clientId) clientIdLength
      -- fetch request
      , build32 defaultReplicaId
      , build32 (fromIntegral timeout) -- max_wait_time
      , build32 defaultMinBytes
      , build32 defaultMaxBytes
      , build8 (isolationLevel ReadUncommitted)
      , build32 fetchSessionId
      , build32 fetchSessionEpoch
      , build32 1 -- number of following topics

      , build16 (size16 topicName)
      , buildArray topicName topicNameSize
      , build32 (fromIntegral partitionCount) -- number of following partitions
      , foldl'
          (\b p -> b <> foldBuilder
              [ build32 (partitionIndex p)
              , build32 defaultCurrentLeaderEpoch
              , build64 (partitionOffset p)
              , build64 defaultLogStartOffset
              , build32 defaultMaxBytes -- partition_max_bytes
              ]
          ) mempty partitions
      , build32 0
      ]
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 1 mempty
      writeUnliftedArray arr 0 requestMetadata
      pure arr
  where
    Topic topicName _ _ = topic
    topicNameSize = sizeofByteArray topicName
    partitionCount = length partitions

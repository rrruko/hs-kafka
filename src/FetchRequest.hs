module FetchRequest
  ( fetchRequest
  , sessionlessFetchRequest
  ) where

import Data.Foldable (foldl')
import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Common
import KafkaWriter

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

maxFetchRequestBytes :: Int32
maxFetchRequestBytes = 30 * 1000 * 1000

fetchRequest ::
     Int32
  -> Int32
  -> Int
  -> Topic
  -> [Partition]
  -> UnliftedArray ByteArray
fetchRequest fetchSessionId fetchSessionEpoch timeout topic partitions =
  let
    requestSize = 53 + 28 * partitionCount + topicNameSize + clientIdLength
    requestMetadata = evaluate $ foldBuilder $
      [ build32 (fromIntegral $ requestSize - 4) -- size
      -- common request headers
      , build16 fetchApiKey
      , build16 fetchApiVersion
      , build32 correlationId
      , build16 (fromIntegral clientIdLength)
      , buildArray (fromByteString clientId) clientIdLength
      -- fetch request
      , build32 (-1) -- replica_id
      , build32 (fromIntegral timeout) -- max_wait_time
      , build32 1 -- min_bytes
      , build32 maxFetchRequestBytes -- max_bytes (not sure what to pass for this)
      , build8 (isolationLevel ReadUncommitted)
      , build32 fetchSessionId
      , build32 fetchSessionEpoch
      , build32 1 -- number of following topics

      , build16 (size16 topicName)
      , buildArray topicName topicNameSize
      , build32 (fromIntegral partitionCount) -- number of following partitions
      , foldl'
          (\b p -> b <> foldBuilder
              [ build32 (partitionIndex p) -- partition
              , build32 0 -- current_leader_epoch
              , build64 (partitionOffset p) -- fetch_offset
              , build64 0 -- log_start_offset
              , build32 maxFetchRequestBytes -- partition_max_bytes
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

module FetchRequest
  ( fetchRequest
  , sessionlessFetchRequest
  ) where

import Control.Monad
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
  -> UnliftedArray ByteArray
sessionlessFetchRequest = fetchRequest 0 (-1)

fetchRequest ::
     Int32
  -> Int32
  -> Int
  -> Topic
  -> UnliftedArray ByteArray
fetchRequest fetchSessionId fetchSessionEpoch timeout topic =
  let
    requestSize = 57 + 28 * partitionCount + topicNameSize + clientIdLength
    requestMetadata = evaluateWriter requestSize $ do
      write32 (fromIntegral $ requestSize - 4) -- size

      -- common request headers
      write16 fetchApiKey
      write16 fetchApiVersion
      write32 correlationId
      write16 (fromIntegral clientIdLength)
      writeArray (fromByteString clientId) clientIdLength

      -- fetch request
      write32 (-1) -- replica_id
      write32 (fromIntegral timeout) -- max_wait_time
      write32 0 -- min_bytes
      write32 (-1) -- max_bytes (not sure what to pass for this)
      write8 (isolationLevel ReadUncommitted)
      write32 fetchSessionId
      write32 fetchSessionEpoch
      write32 1 -- number of following topics

      write16 (size16 topicName)
      writeArray topicName topicNameSize
      write32 (fromIntegral partitionCount) -- number of following partitions

      forM_ [0..fromIntegral partitionCount - 1] $ \p -> do
        write32 p -- partition
        write32 0 -- current_leader_epoch
        write64 0 -- fetch_offset
        write64 0 -- log_start_offset
        write32 (-1) -- partition_max_bytes

      write32 0 -- forgotten_topics_data array length
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 1 mempty
      writeUnliftedArray arr 0 requestMetadata
      pure arr
  where
    Topic topicName partitionCount _ = topic
    topicNameSize = sizeofByteArray topicName

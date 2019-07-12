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
    requestMetadata = evaluateWriter 10 $ do
      writeBE32 10 -- size

      -- common request headers
      writeBE16 fetchApiKey
      writeBE16 fetchApiVersion
      writeBE32 correlationId
      writeBE16 (fromIntegral clientIdLength)
      writeArray (fromByteString clientId) clientIdLength
      writeBE32 (-1)

      -- fetch request
      writeBE32 (-1) -- replica_id
      writeBE32 (fromIntegral timeout) -- max_wait_time
      writeBE32 0 -- min_bytes
      writeBE32 (-1) -- max_bytes (not sure what to pass for this)
      write8 (isolationLevel ReadUncommitted)
      writeBE32 fetchSessionId
      writeBE32 fetchSessionEpoch
      writeBE32 1 -- number of following topics

      writeBE16 (size16 topicName)
      writeArray topicName topicNameSize
      writeBE32 (fromIntegral partitionCount) -- number of following partitions

      forM_ [0..fromIntegral partitionCount] $ \p -> do
        writeBE32 p -- partition
        writeBE32 0 -- current_leader_epoch
        writeBE64 0 -- fetch_offset
        writeBE64 0 -- log_start_offset
        writeBE32 (-1) -- partition_max_bytes

      writeBE32 0 -- forgotten_topics_data array length
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 1 mempty
      writeUnliftedArray arr 0 requestMetadata
      pure arr
  where
    Topic topicName partitionCount _ = topic
    topicNameSize = sizeofByteArray topicName

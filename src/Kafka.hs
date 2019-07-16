module Kafka
  ( produce
  , fetch
  , listOffsets
  ) where

import Data.Bifunctor (first)
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Socket.Stream.Uninterruptible.Bytes

import Common
import FetchRequest
import ListOffsetsRequest
import ProduceRequest

request ::
     Kafka
  -> UnliftedArray ByteArray
  -> IO (Either KafkaException ())
request kafka msg = first toKafkaException <$> sendMany (getKafka kafka) msg

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException ())
produce kafka topic@(Topic _ parts ctr) waitTime payloads = do
  p <- fromIntegral <$> readIORef ctr
  let message = produceRequest (waitTime `div` 1000) topic p payloads
  e <- request kafka message
  either (pure . Left) (\a -> increment parts ctr >> pure (Right a)) e

--
-- [Note: Partitioning algorithm]
-- We increment the partition pointer after each batch
-- production. We currently do not handle overflow of partitions
-- (i.e. we don't pay attention to max_bytes)
increment :: ()
  => Int -- ^ number of partitions
  -> IORef Int -- ^ incrementing number
  -> IO ()
increment totalParts ref = modifyIORef' ref $ \ptr ->
  -- 0-indexed vs 1-indexed
  -- (ptr vs total number of partitions)
  if ptr + 1 == totalParts
    then 0
    else ptr + 1

fetch ::
     Kafka
  -> Topic
  -> Int
  -> [Partition]
  -> IO (Either KafkaException ())
fetch kafka topic waitTime partitions =
  request kafka $ sessionlessFetchRequest (waitTime `div` 1000) topic partitions

listOffsets ::
     Kafka
  -> Topic
  -> [Partition]
  -> IO (Either KafkaException ())
listOffsets kafka topic partitions = do
  request kafka $ listOffsetsRequest topic partitions

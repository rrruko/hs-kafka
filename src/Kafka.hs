module Kafka where

import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import GHC.Conc
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4

import Common
import ProduceRequest

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException ())
produce kafka topic@(Topic _ parts ctr) waitTime payloads = do
  interrupt <- registerDelay waitTime
  let message = produceRequest (waitTime `div` 1000) topic payloads
  e <- first toKafkaException <$> sendProduceRequest kafka interrupt message
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

produce' :: UnliftedArray ByteArray -> ByteString -> IO ()
produce' bytes topicName = do
  topic <- Topic (fromByteString topicName) 0 <$> newIORef 0
  Right k <- newKafka (Peer (IPv4 0) 9092)
  _ <- produce k topic 30000000 bytes
  pure ()


module Kafka
  ( produce
  , fetch
  ) where

import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4
import Socket.Stream.Uninterruptible.Bytes

import Common
import FetchRequest
import ProduceRequest

produce ::
     Kafka
  -> Topic
  -> Int -- number of microseconds to wait for response
  -> UnliftedArray ByteArray -- payloads
  -> IO (Either KafkaException ())
produce kafka topic waitTime payloads = do
  let message = produceRequest (waitTime `div` 1000) topic payloads
  first toKafkaException <$> sendMany (getKafka kafka) message

fetch ::
     Kafka
  -> Topic
  -> Int
  -> IO (Either KafkaException ())
fetch kafka topic waitTime = do
  let message = sessionlessFetchRequest (waitTime `div` 1000) topic
  first toKafkaException <$> sendMany (getKafka kafka) message

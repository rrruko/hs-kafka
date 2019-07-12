module Kafka
  ( produce
  , fetch
  ) where

import Data.Bifunctor (first)
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Socket.Stream.Uninterruptible.Bytes

import Common
import FetchRequest
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
produce kafka topic waitTime payloads =
  request kafka $ produceRequest (waitTime `div` 1000) topic payloads

fetch ::
     Kafka
  -> Topic
  -> Int
  -> IO (Either KafkaException ())
fetch kafka topic waitTime =
  request kafka $ sessionlessFetchRequest (waitTime `div` 1000) topic

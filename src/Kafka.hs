module Kafka where

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

produce' :: UnliftedArray ByteArray -> ByteString -> IO ()
produce' bytes topicName = do
  topic <- Topic (fromByteString topicName) 0 <$> newIORef 0
  Right k <- newKafka (Peer (IPv4 0) 9092)
  _ <- produce k topic 30000000 bytes
  pure ()

fetch ::
     Kafka
  -> Topic
  -> Int
  -> IO (Either KafkaException ())
fetch kafka topic waitTime = do
  let message = sessionlessFetchRequest (waitTime `div` 1000) topic
  first toKafkaException <$> sendMany (getKafka kafka) message

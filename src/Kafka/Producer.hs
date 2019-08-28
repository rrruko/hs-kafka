module Kafka.Producer
  ( Producer
  , newProducer
  , produce
  ) where

import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Socket.Stream.IPv4 (Peer)

import Kafka.Common
import Kafka.Internal.Topic (makeTopic)

import qualified Kafka.Internal.Request as Request

data Producer = Producer Kafka Topic

newProducer :: Peer -> TopicName -> IO (Either KafkaException Producer)
newProducer peer topicName = do
  kafka <- newKafka peer
  case kafka of
    Left err -> pure (Left err)
    Right k -> do
      top <- makeTopic k topicName
      case top of
        Left err -> pure (Left err)
        Right t -> pure (Right (Producer k t))

produce :: 
     Producer 
  -> Int 
  -> UnliftedArray ByteArray 
  -> IO (Either KafkaException ())
produce (Producer k t) = Request.produce k t

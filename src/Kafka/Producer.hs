module Kafka.Producer
  ( Producer(..)
  , newProducer
  , produce
  ) where

import Data.IORef
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Socket.Stream.IPv4 (Peer)

import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Internal.Topic (makeTopic)

import qualified Kafka.Internal.Request as Request

data Producer = Producer
  { producerKafka :: Kafka
  , producerTopics :: IORef (Map.Map TopicName Topic)
  , producerTimeout :: Int
  }

newProducer :: Peer -> Int -> IO (Either KafkaException Producer)
newProducer peer timeout = do
  kafka <- newKafka peer
  case kafka of
    Left err -> pure (Left err)
    Right k -> do
      tops <- newIORef mempty
      pure (Right (Producer k tops timeout))

produce :: 
     Producer 
  -> TopicName
  -> UnliftedArray ByteArray 
  -> IO (Either KafkaException ())
produce (Producer k t timeout) topicName msgs = do
  tops <- readIORef t
  case Map.lookup topicName tops of
    Just topicState -> Request.produce k topicState timeout msgs
    Nothing -> do
      newTopic <- makeTopic k topicName
      case newTopic of
        Right top -> do
          modifyIORef t (Map.insert topicName top)
          Request.produce k top timeout msgs
        Left err -> pure (Left err)

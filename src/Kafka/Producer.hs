{-# language
    BangPatterns
  #-}

module Kafka.Producer
  ( Producer(..)
  , newProducer
  , produce
  ) where

import Data.IORef
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Socket.Stream.IPv4 (Peer)
import System.IO (Handle)

import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Internal.Request.Types
import Kafka.Internal.Topic (makeTopic)

import qualified Kafka.Internal.Request as Request

data Producer = Producer
  { producerKafka :: !Kafka
    -- ^ Connection to Kafka
  , producerTopics :: !(IORef (Map.Map TopicName Topic))
    -- ^ TopicName with associated Topic
  , producerTimeout :: !Int
    -- ^ Timeout in microseconds
  , producerDebugHandle :: !(Maybe Handle)
    -- ^ File handle for debug output
  }

-- | Attempt to establish a connection to Kafka.
newProducer :: Peer -> Int -> Maybe Handle -> IO (Either KafkaException Producer)
newProducer peer timeout handle = do
  kafka <- newKafka peer
  case kafka of
    Left err -> pure (Left err)
    Right k -> do
      tops <- newIORef mempty
      pure (Right (Producer k tops timeout handle))

-- | Send messages to Kafka.
produce ::
     Producer -- ^ Producer
  -> TopicName -- ^ Topic to which we push
  -> UnliftedArray ByteArray -- ^ Messages
  -> IO (Either KafkaException ())
produce (Producer k t timeout handle) topicName msgs = do
  tops <- readIORef t
  case Map.lookup topicName tops of
    Just topicState -> Request.produce k (ProduceRequest topicState timeout msgs) handle
    Nothing -> do
      newTopic <- makeTopic k topicName handle
      case newTopic of
        Right top -> do
          modifyIORef t (Map.insert topicName top)
          Request.produce k (ProduceRequest top timeout msgs) handle
        Left err -> pure (Left err)

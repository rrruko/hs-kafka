{-# language
    BangPatterns
  , LambdaCase
  #-}

module Kafka.Producer
  ( Producer(..)
  , withProducer
  , produce
  ) where

import Data.Primitive.Unlifted.Array
import GHC.Conc (registerDelay)
import Socket.Stream.IPv4 (Peer)
import System.IO (Handle)

import qualified Data.Map.Strict as Map

import Kafka.Common
import Kafka.Internal.Produce.Response
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

withProducer :: ()
  => Peer
  -> Int
  -> Maybe Handle
  -> (Producer -> IO a)
  -> IO (Either KafkaException a)
withProducer peer timeout h f = withKafka peer $ \k -> do
  topics <- newIORef mempty
  let producer = Producer k topics timeout h
  f producer

produce' ::
     Producer
  -> Topic
  -> UnliftedArray ByteArray
  -> IO (Either KafkaException ())
produce' (Producer k _ timeout handle) topic msgs = do
  status <- Request.produce k (ProduceRequest topic timeout msgs) handle
  case status of
    Left err -> pure (Left err)
    Right () -> do
      interrupt <- registerDelay 30000000
      getProduceResponse k interrupt handle >>= \case
        Left err -> pure (Left err)
        Right (Left msg) -> pure (Left (KafkaParseException msg))
        Right (Right resp) ->
          let errs =
                [ prResponseErrorCode r
                | x <- produceResponseMessages resp
                , r <- prPartitionResponses x
                , prResponseErrorCode r /= 0
                ]
           in case errs of
                [] -> pure (Right ())
                err : _ -> case fromErrorCode err of
                  -- TODO: should this be turned into `UnknownServerError`?
                  Nothing -> pure (Right ())
                  Just e -> pure (Left (KafkaProtocolException e))

-- | Send messages to Kafka.
produce ::
     Producer -- ^ Producer
  -> TopicName -- ^ Topic to which we push
  -> UnliftedArray ByteArray -- ^ Messages
  -> IO (Either KafkaException ())
produce producer@(Producer k t _ handle) topicName msgs = do
  tops <- readIORef t
  case Map.lookup topicName tops of
    Just topicState -> produce' producer topicState msgs
    Nothing -> do
      newTopic <- makeTopic k topicName handle
      case newTopic of
        Right top -> do
          modifyIORef t (Map.insert topicName top)
          produce' producer top msgs
        Left err -> pure (Left err)

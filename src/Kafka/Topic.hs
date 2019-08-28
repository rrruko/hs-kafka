{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Topic
  ( makeTopic
  , getPartitionCount
  ) where

import Control.Concurrent.STM.TVar
import Control.Monad.Except
import Data.Coerce
import Data.IORef
import Data.Int
import Data.List

import Kafka.Common
import Kafka.Internal.Response
import Kafka.Internal.Request

import qualified Kafka.Internal.Metadata.Response as M

makeTopic :: Kafka -> TopicName -> IO (Either KafkaException Topic)
makeTopic kafka topicName = do
  partitionCounter <- liftIO (newIORef 0)
  getPartitionCount kafka topicName 5000000 >>= \case
    Right count -> pure (Right (Topic (coerce topicName) (fromIntegral count) partitionCounter))
    Left err -> pure (Left err)

getPartitionCount :: Kafka -> TopicName -> Int -> IO (Either KafkaException Int32)
getPartitionCount kafka topicName timeout = runExceptT $ do
  _ <- ExceptT $ metadata kafka topicName NeverCreate
  interrupt <- liftIO $ registerDelay timeout
  parts <- fmap (metadataPartitions topicName) $ ExceptT $
    tryParse <$> M.getMetadataResponse kafka interrupt
  case parts of
    Just p -> pure p
    Nothing -> throwError $
      KafkaException "Topic name not found in metadata request"

metadataPartitions :: TopicName -> M.MetadataResponse -> Maybe Int32
metadataPartitions topicName mdr =
  let tops = M.topics mdr
  in  case find ((== topicName) . coerce . fromByteString . M.name) tops of
        Just top -> Just (fromIntegral $ length (M.partitions top))
        Nothing -> Nothing

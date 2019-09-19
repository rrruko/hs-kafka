{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Topic
  ( makeTopic
  , getPartitionCount
  ) where

import Control.Monad.Except
import Data.List
import System.IO (Handle)

import Kafka.Common
import Kafka.Internal.Response
import Kafka.Internal.Request
import Kafka.Internal.Request.Types

import qualified Kafka.Internal.Metadata.Response as M

makeTopic :: Kafka -> TopicName -> Maybe Handle -> IO (Either KafkaException Topic)
makeTopic kafka topicName handle = do
  partitionCounter <- liftIO (newIORef 0)
  getPartitionCount kafka topicName 5000000 handle >>= \case
    Right count -> pure (Right (Topic (coerce topicName) (fromIntegral count) partitionCounter))
    Left err -> pure (Left err)

getPartitionCount :: Kafka -> TopicName -> Int -> Maybe Handle -> IO (Either KafkaException Int32)
getPartitionCount kafka topicName timeout handle = runExceptT $ do
  _ <- ExceptT $ metadata kafka (MetadataRequest topicName NeverCreate) handle
  interrupt <- liftIO $ registerDelay timeout
  parts <- fmap (metadataPartitions topicName) $ ExceptT $
    tryParse <$> M.getMetadataResponse kafka interrupt handle
  case parts of
    Just p -> pure p
    Nothing -> throwError $
      KafkaException "Topic name not found in metadata request"

metadataPartitions :: TopicName -> M.MetadataResponse -> Maybe Int32
metadataPartitions topicName mdr =
  let tops = M.topics mdr
  in  case find ((== topicName) . M.name) tops of
        Just top -> Just (fromIntegral $ length (M.partitions top))
        Nothing -> Nothing

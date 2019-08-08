{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.Int
import GHC.Conc

import Kafka
import Kafka.Common
import Kafka.ListOffsets.Response

main :: IO ()
main = do
  sendListOffsetsRequest testTopicName [0, 1]

testTopicName :: TopicName
testTopicName = TopicName (fromByteString "test")

thirtySecondsUs :: Int
thirtySecondsUs = 30000000

sendListOffsetsRequest :: TopicName -> [Int32] -> IO ()
sendListOffsetsRequest topicName partitionIndices = do
  withDefaultKafka $ \kafka -> do
    listOffsets kafka topicName partitionIndices Latest >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getListOffsetsResponse kafka interrupt
        print response
      Left exception -> do
        print exception

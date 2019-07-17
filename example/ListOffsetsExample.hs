{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.Int
import Data.IORef
import GHC.Conc

import Kafka
import Kafka.Common
import Kafka.ListOffsets.Response

main :: IO ()
main = do
  topic' <- testTopic <$> newIORef 0
  sendListOffsetsRequest topic' [0, 1]

testTopic :: IORef Int -> Topic
testTopic = Topic (fromByteString "test") 1

thirtySecondsUs :: Int
thirtySecondsUs = 30000000

sendListOffsetsRequest :: Topic -> [Int32] -> IO ()
sendListOffsetsRequest topic' partitionIndices = do
  withDefaultKafka $ \kafka -> do
    listOffsets kafka topic' partitionIndices >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getListOffsetsResponse kafka interrupt
        print response
      Left exception -> do
        print exception

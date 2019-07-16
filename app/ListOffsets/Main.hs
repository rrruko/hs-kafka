{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.IORef
import GHC.Conc

import Chronos (now)
import Common
import Kafka
import ListOffsetsResponse

main :: IO ()
main = do
  topic' <- testTopic <$> newIORef 0
  sendListOffsetsRequest topic' [Partition 0 0, Partition 1 0]

testTopic :: IORef Int -> Topic
testTopic = Topic (fromByteString "test") 1

thirtySecondsUs :: Int
thirtySecondsUs = 30000000

sendListOffsetsRequest :: Topic -> [Partition] -> IO ()
sendListOffsetsRequest topic' partitions = do
  time <- now
  withDefaultKafka $ \kafka -> do
    listOffsets kafka topic' partitions time >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getListOffsetsResponse kafka interrupt
        print response
      Left exception -> do
        print exception

{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.ByteString (ByteString)
import Data.IORef
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import GHC.Conc

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.Produce.Response

main :: IO ()
main = do
  putStrLn "Produce request:"
  ctr <- newIORef 0
  response <- sendProduceRequest (testTopic ctr)
  case response of
    Right parseResult -> do
      case parseResult of
        Right res@(ProduceResponse [ProduceResponseMessage _ rs] _) -> do
          print res
          putStrLn "Fetch request:"
          sendFetchRequest (TopicName testTopicName)
            (fmap
              (\r -> Partition
                (prResponsePartition r)
                (prResponseBaseOffset r))
              rs)
        Right resp -> do
          putStrLn $
               "Got an unexpected number of responses from the produce "
            <> "request"
          print resp
        Left parseError -> do
          putStrLn "Failed to parse the response"
          putStrLn parseError
    Left networkError -> do
      putStrLn "Failed to communicate with the Kafka server"
      print networkError

testTopicName :: ByteArray
testTopicName = fromByteString "test"

testTopic :: IORef Int -> Topic
testTopic = Topic testTopicName 1

thirtySecondsUs :: Int
thirtySecondsUs = 30000000

byteStrings :: [ByteString] -> UnliftedArray ByteArray
byteStrings = unliftedArrayFromList . fmap fromByteString

sendProduceRequest :: 
     Topic 
  -> IO (Either KafkaException (Either String ProduceResponse))
sendProduceRequest topic = do
  withDefaultKafka $ \kafka -> do
    let msg = byteStrings ["aaaaa", "bbbbb", "ccccc"]
    produce kafka topic thirtySecondsUs msg >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getProduceResponse kafka interrupt
        pure response
      Left exception -> do
        pure (Left exception)

sendFetchRequest :: TopicName -> [Partition] -> IO ()
sendFetchRequest topicName partitions = do
  withDefaultKafka $ \kafka -> do
    fetch kafka topicName thirtySecondsUs partitions >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getFetchResponse kafka interrupt
        print response
      Left exception -> do
        print exception

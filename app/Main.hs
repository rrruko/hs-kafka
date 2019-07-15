{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.Int
import Data.IORef
import Data.Primitive.Unlifted.Array
import GHC.Conc
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4 (Peer(..))

import Common
import Kafka
import ProduceResponse
import FetchResponse

main :: IO ()
main = do
  putStrLn "produce request:"
  response <- sendProduceRequest
  case response of
    Right parseResult -> do
      case parseResult of
        Right res@(ProduceResponse [ProduceResponseMessage _ [resp]] _) -> do
          print res
          putStrLn "fetch request:"
          sendFetchRequest (prResponseBaseOffset resp)
        Right resp -> do
          putStrLn "Got an unexpected number of responses from the produce request"
          print resp
        Left parseError -> do
          putStrLn "Failed to parse the response: "
          putStrLn parseError
    Left networkError -> do
      putStrLn "Failed to communicate with the Kafka server: "
      print networkError

testTopic :: IO Topic
testTopic = Topic (fromByteString "test") 1 <$> newIORef 0

sendProduceRequest :: IO (Either KafkaException (Either String ProduceResponse))
sendProduceRequest = do
  let thirtySecondsUs = 30000000
  withKafka $ \kafka -> do
    partitionIndex <- newIORef 0
    topic <- testTopic
    let msg = unliftedArrayFromList
          [ fromByteString "aaaaa"
          , fromByteString "bbbbb"
          , fromByteString "ccccc"
          ]
    produce kafka topic thirtySecondsUs msg >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getProduceResponse kafka interrupt
        pure response
      Left exception -> do
        pure (Left exception)

sendFetchRequest :: Int64 -> IO ()
sendFetchRequest offset = do
  let thirtySecondsUs = 30000000
  withKafka $ \kafka -> do
    partitionIndex <- newIORef 0
    topic <- testTopic
    fetch kafka topic thirtySecondsUs offset >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getFetchResponse kafka interrupt
        print response
      Left exception -> do
        print exception

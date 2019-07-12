{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

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
  print "produce request:"
  sendProduceRequest
  print "fetch request:"
  sendFetchRequest

testTopic :: IO Topic
testTopic = Topic (fromByteString "test") 0 <$> newIORef 0


withKafka :: (Kafka -> IO a) -> IO a
withKafka f = do
  newKafka (Peer (IPv4 0) 9092) >>= \case
    Right kafka -> do
      f kafka
    Left bad -> do
      print bad
      fail "Couldn't connect to kafka"

sendProduceRequest :: IO ()
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
        print response
      Left exception -> do
        print exception

sendFetchRequest :: IO ()
sendFetchRequest = do
  let thirtySecondsUs = 30000000
  withKafka $ \kafka -> do
    partitionIndex <- newIORef 0
    topic <- testTopic
    fetch kafka topic thirtySecondsUs >>= \case
      Right () -> do
        interrupt <- registerDelay thirtySecondsUs
        response <- getFetchResponse kafka interrupt
        print response
      Left exception -> do
        print exception

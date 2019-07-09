{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString (ByteString)
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import GHC.Conc
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4 (Peer(..))

import Common
import Kafka
import ProduceResponse

main :: IO ()
main = do
  let thirtySecondsUs = 30000000
  newKafka (Peer (IPv4 0) 9092) >>= \case
    Right kafka -> do
      partitionIndex <- newIORef (0 :: Int)
      let topic = Topic (byteArrayFromByteString "test") 0 partitionIndex
      let msg = unliftedArrayFromList
            [ fromByteString "aaaaa"
            , fromByteString "bbbbb"
            , fromByteString "ccccc"
            ]
      v <- produce kafka topic thirtySecondsUs msg
      case v of
        Right () -> do
          interrupt <- registerDelay thirtySecondsUs
          response <- getProduceResponse kafka interrupt
          print response
        Left exception -> do
          print exception
    Left bad -> do
      print bad
      fail "Couldn't connect to kafka"

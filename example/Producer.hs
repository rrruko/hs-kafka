{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Data.ByteString (ByteString)
import Data.IORef
import Data.Primitive.ByteArray (ByteArray)
import Data.Primitive.Unlifted.Array
import System.Random

import qualified Data.ByteString.Char8 as B

import Kafka
import Kafka.Common

main :: IO ()
main = producer

setup :: ByteArray -> Int -> IO (Topic, Maybe Kafka)
setup topicName partitionCount = do
  currentPartition <- newIORef 0
  let t = Topic topicName partitionCount currentPartition
  k <- newKafka defaultKafka
  pure (t, either (const Nothing) Just k)

byteStrings :: [ByteString] -> UnliftedArray ByteArray
byteStrings = unliftedArrayFromList . fmap fromByteString

names :: [ByteString]
names = 
  [ "bulbasaur"
  , "ivysaur"
  , "venusaur"
  , "charmander"
  , "charmeleon"
  , "charizard"
  , "squirtle"
  , "wartortle"
  , "blastoise"
  , "caterpie"
  , "metapod"
  , "butterfree"
  , "weedle"
  , "kakuna"
  , "beedrill"
  ]

pick :: RandomGen g => [a] -> g -> (a, g)
pick xs g = (xs !! randomIndex, g')
  where
  (randomIndex, g') = randomR (0, length xs - 1) g

pickMany :: RandomGen g => Int -> [a] -> g -> ([a], g)
pickMany 0 _ g = ([], g)
pickMany n xs g = 
  let (e, g') = pick xs g
      (rest, g'') = pickMany (n-1) xs g'
  in  (e:rest, g'')

producer :: IO ()
producer = do
  (t, kafka) <- setup (fromByteString "example-consumer-group") 8
  rand <- getStdGen
  case kafka of
    Nothing -> putStrLn "Failed to connect to kafka"
    Just k -> loop k t rand

loop :: Kafka -> Topic -> StdGen -> IO ()
loop k t rand = do
  let (pokes, rand') = pickMany 10000 names rand
  _ <- produce k t 5000000 (byteStrings pokes)
  B.putStrLn ("sent " <> B.pack (show (B.length (B.concat pokes))) <> " bytes")
  threadDelay 1000000
  loop k t rand'

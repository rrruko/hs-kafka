{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import System.Random

import Kafka.Common
import Kafka.Producer

names :: [String]
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

main :: IO ()
main = do
  p <- newProducer defaultKafka (mkTopicName "example-consumer-group")
  rand <- getStdGen
  case p of
    Left err -> putStrLn $ "Failed to create producer (" <> show err <> ")"
    Right prod -> loop prod rand

loop :: Producer -> StdGen -> IO ()
loop p rand = do
  let (pokes, rand') = pickMany 10000 names rand
  _ <- produce p 5000000 (messages pokes)
  putStrLn ("sent " <> show (length (concat pokes)) <> " bytes")
  threadDelay 1000000
  loop p rand'

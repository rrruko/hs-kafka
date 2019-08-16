{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Maybe
import Net.IPv4 (ipv4)
import Socket.Stream.IPv4 (Peer(..))
import System.IO.Unsafe (unsafePerformIO)

import qualified Data.ByteString.Char8 as B

import Kafka.Common
import Kafka.Consumer
import Kafka.Fetch.Response (FetchResponse)

import qualified Kafka.Fetch.Response as F

children :: MVar [MVar ()]
{-# NOINLINE children #-}
children = unsafePerformIO (newMVar [])

waitForChildren :: IO ()
waitForChildren = do
  cs <- takeMVar children
  case cs of
    [] -> pure ()
    m:ms -> do
      putMVar children ms
      takeMVar m
      waitForChildren

main :: IO ()
main = do
  interrupt <- newTVarIO False
  fork (consumer interrupt) "C1"
  fork (consumer interrupt) "C2"
  fork (consumer interrupt) "C3"
  putStrLn "Press enter to quit"
  _ <- getLine
  atomically $ writeTVar interrupt True

fork :: (String -> IO ()) -> String -> IO ()
fork f name = do
  mvar <- newEmptyMVar
  childs <- takeMVar children
  putMVar children (mvar:childs)
  void $ forkFinally (f name) (\_ -> putMVar mvar ())

consumer :: TVar Bool -> String -> IO ()
consumer interrupt name = do
  kaf <- newKafka (Peer (ipv4 10 10 10 234) 9092)
  case kaf of
    Left e -> putStrLn ("failed to connect (" <> show e <> ")")
    Right k -> do
      let settings = ConsumerSettings
            { csTopicName = TopicName (fromByteString "diamond")
            , groupName = fromByteString "ruko-diamond"
            , maxFetchBytes = 30000
            , groupFetchStart = Earliest
            , defaultTimeout = 5000000
            }
      newConsumer k settings >>= \case
        Left err -> putStrLn ("Failed to create consumer: " <> show err)
        Right c -> 
          runExceptT (runConsumer (loop interrupt c)) >>= \case
            Right () -> putStrLn "Finished with no errors."
            Left err -> putStrLn ("Consumer died with: " <> show err)
  where

loop :: TVar Bool -> ConsumerState -> Consumer ()
loop interrupt c = do
  if (null (offsets c))
    then do
      liftIO (putStrLn "No offsets assigned; quitting")
      leave c
    else do
      (resp, c) <- getRecordSet 1000000 c
      liftIO do
        print resp
        traverse_ B.putStrLn (fetchResponseContents resp)
      commitOffsets c
      i <- liftIO (readTVarIO interrupt)
      if i
        then leave c
        else loop interrupt c

fetchResponseContents :: FetchResponse -> [ByteString]
fetchResponseContents fetchResponse =
    mapMaybe F.recordValue
  . concatMap F.records
  . concat
  . mapMaybe F.recordSet
  . concatMap F.partitionResponses
  . F.responses
  $ fetchResponse

{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.Coerce
import Data.Maybe
import Net.IPv4 (ipv4)
import Socket.Stream.IPv4 (Peer(..))
import System.IO.Unsafe (unsafePerformIO)

import qualified Data.ByteString.Char8 as B

import Kafka.Common
import Kafka.Consumer

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
  interrupt <- newTVarIO Uninterrupted
  fork (consumer interrupt)
  fork (consumer interrupt)
  fork (consumer interrupt)
  threadDelay 15000000
  fork (consumer interrupt)
  putStrLn "Press enter to quit"
  _ <- getLine
  atomically $ writeTVar interrupt Interrupted
  waitForChildren

fork :: IO () -> IO ()
fork f = do
  mvar <- newEmptyMVar
  childs <- takeMVar children
  putMVar children (mvar:childs)
  void $ forkFinally f (\_ -> putMVar mvar ())

consumer :: TVar Interruptedness -> IO ()
consumer interrupt = do
  kaf <- newKafka (Peer (ipv4 0 0 0 0) 9092)
  case kaf of
    Left e -> putStrLn ("failed to connect (" <> show e <> ")")
    Right k -> do
      let diamondSettings = ConsumerSettings
            { csTopicName = TopicName (fromByteString "diamond")
            , groupName = fromByteString "ruko-diamond"
            , maxFetchBytes = 30000
            , groupFetchStart = Earliest
            , timeout = 5000000
            , autoCommit = AutoCommit
            , handle = Nothing
            }
      void $ newConsumer k diamondSettings >>= \case
        Left err -> putStrLn ("Failed to create consumer: " <> show err)
        Right c -> do
          evalConsumer c (loop interrupt) >>= \case
            Right () -> putStrLn "Finished with no errors."
            Left err -> putStrLn ("Consumer died with: " <> show err)

loop :: TVar Interruptedness -> Consumer ()
loop interrupt = do
  o <- getsv offsets
  if (null o)
    then do
      liftIO (putStrLn "No offsets assigned; quitting")
      leave
    else do
      resp <- getRecordSet 1000000
      o' <- getsv offsets
      GroupMember _ m <- getsv member
      liftIO $ B.putStrLn $
        "Got: "
        <> B.intercalate ", " (fetchResponseContents resp)
        <> "(" <> B.pack (show o') <> ") "
        <> "(" <> maybe "NULL" toByteString (coerce m) <> ")"
      i <- liftIO (readTVarIO interrupt)
      case i of
        Interrupted -> leave
        Uninterrupted -> loop interrupt

fetchResponseContents :: FetchResponse -> [ByteString]
fetchResponseContents fetchResponse =
    mapMaybe recordValue
  . concatMap records
  . concat
  . mapMaybe recordSet
  . concatMap partitions
  . topics
  $ fetchResponse

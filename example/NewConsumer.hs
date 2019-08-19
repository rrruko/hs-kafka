{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Chronos
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.IORef
import Data.Maybe
import Net.IPv4 (ipv4)
import Socket.Stream.IPv4 (Peer(..))
import System.IO.Unsafe (unsafePerformIO)
import Torsor

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
  waitForChildren

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
      let diamondSettings = ConsumerSettings
            { csTopicName = TopicName (fromByteString "diamond")
            , groupName = fromByteString "ruko-diamond"
            , maxFetchBytes = 30000
            , groupFetchStart = Earliest
            , defaultTimeout = 5000000
            , autoCommit = AutoCommit
            }
      newConsumer k diamondSettings >>= \case
        Left err -> putStrLn ("Failed to create consumer: " <> show err)
        Right c -> do
          t <- newIORef epoch
          evalConsumer c (loop interrupt t) >>= \case
            Right ((), _) -> putStrLn "Finished with no errors."
            Left err -> putStrLn ("Consumer died with: " <> show err)

loop :: TVar Bool -> IORef Time -> Consumer ()
loop interrupt timeSinceHeartbeat = do
  o <- gets offsets
  if (null o)
    then do
      liftIO (putStrLn "No offsets assigned; quitting")
      leave
    else do
      resp <- getRecordSet 1000000
      liftIO $ traverse_ B.putStrLn (fetchResponseContents resp)
      currentTime <- liftIO now
      timeSince <- liftIO $ readIORef timeSinceHeartbeat
      when (currentTime >= add (scale 3 second) timeSince) do
        liftIO $ writeIORef timeSinceHeartbeat currentTime
        liftIO $ putStrLn "[Heartbeat]"
        void sendHeartbeat
      i <- liftIO (readTVarIO interrupt)
      if i
        then leave
        else loop interrupt timeSinceHeartbeat

fetchResponseContents :: FetchResponse -> [ByteString]
fetchResponseContents fetchResponse =
    mapMaybe F.recordValue
  . concatMap F.records
  . concat
  . mapMaybe F.recordSet
  . concatMap F.partitions
  . F.topics
  $ fetchResponse

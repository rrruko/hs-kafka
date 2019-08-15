{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.IORef
import Data.Maybe
import Data.Primitive.ByteArray (ByteArray)
import GHC.Conc
import Net.IPv4 (ipv4)
import Socket.Stream.IPv4 (Peer(..))
import System.IO.Unsafe (unsafePerformIO)

import Kafka.Common
import Kafka.Consumer
import Kafka.Fetch.Response (FetchResponse)

import qualified Kafka.Fetch.Response as F

groupName :: ByteArray
groupName = fromByteString "ruko-diamond"

children :: MVar [MVar ()]
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
            , csGroupName = groupName
            , csMaxFetchBytes = 300
            , csGroupFetchStart = Earliest
            , defaultTimeout = 5000000
            }
      c <- newConsumer k settings
      putStrLn "hehehe"
      case c of
        Right c' -> go c' *> putStrLn "done"
        Left err -> print err
  where
  go c' = runExceptT $ runConsumer $ do
    liftIO $ print (offsets c')
    (resp, newThing) <- getRecordSet 1000000 c'
    liftIO $ print resp
    liftIO $ print newThing
    commitOffsets newThing
    leave newThing

_callback :: String -> FetchResponse -> IO ()
_callback name response = 
  putStrLn (name <> ": got " <> show (length (fetchResponseContents response)) <> " messages")

fetchResponseContents :: FetchResponse -> [ByteString]
fetchResponseContents fetchResponse =
    mapMaybe F.recordValue
  . concatMap F.records
  . concat
  . mapMaybe F.recordSet
  . concatMap F.partitionResponses
  . F.responses
  $ fetchResponse

thirtySeconds :: Int
thirtySeconds = 30000000

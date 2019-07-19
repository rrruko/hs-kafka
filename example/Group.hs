{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Monad
import Data.Either (isLeft)
import Data.IORef
import Data.Primitive.ByteArray (ByteArray)
import GHC.Conc
import System.IO.Unsafe (unsafePerformIO)

import Kafka
import Kafka.Common
import Kafka.Heartbeat.Response
import Kafka.JoinGroup.Response
import Kafka.SyncGroup.Response

groupName :: ByteArray
groupName = fromByteString "example-consumer-group"

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
  forkFollower (follower "1")
  forkFollower (follower "2")
  leader
  waitForChildren

forkFollower :: IO () -> IO ()
forkFollower io = do
  mvar <- newEmptyMVar
  childs <- takeMVar children
  putMVar children (mvar:childs)
  void $ forkFinally io (\_ -> putMVar mvar ())

leader :: IO ()
leader = do
  (t, kafka) <- setup groupName
  let
    Topic topicName partitionCount _ = t
    partitions groupMembers i =
      filter
        (\n -> mod (n + i) (length groupMembers) == 0)
        [0..partitionCount-1]
    assign groupMembers =
      fmap
        (\(member, i) ->
          MemberAssignment (fromByteString $ groupMemberId member)
            [TopicAssignment (topicName) (fromIntegral <$> partitions groupMembers i)])
        (zip groupMembers [0..])
  case kafka of
    Nothing -> putStrLn "Failed to connect to kafka"
    Just k -> do
      let member = GroupMember groupName Nothing
      (genId, newMember, allMembers) <- initGroupConsumer k t member
      interrupt <- registerDelay 30000000
      putStrLn "Leader is making the following assignments:"
      print (assign allMembers)
      void $ syncGroup k newMember genId $ assign allMembers
      registerDelay 30000000 >>= getSyncGroupResponse k >>= \case
        Right (Right sgr) -> do
          putStrLn ("leader: my assigned partitions are " <> show (memberAssignment sgr))
          heartbeats k t newMember genId interrupt "leader"
        e -> do
          print e

follower :: String -> IO ()
follower name = do
  (t, kafka) <- setup groupName
  case kafka of
    Nothing -> putStrLn "Failed to connect to kafka"
    Just k -> do
      let member = GroupMember groupName Nothing
      (genId, newMember, _) <- initGroupConsumer k t member
      interrupt <- registerDelay 30000000
      void $ syncGroup k newMember genId
        []
      registerDelay 30000000 >>= getSyncGroupResponse k >>= \case
        Right (Right sgr) -> do
          putStrLn (name <> ": my assigned partitions are " <> show (memberAssignment sgr))
          heartbeats k t newMember genId interrupt name
        e -> do
          print e

setup :: ByteArray -> IO (Topic, Maybe Kafka)
setup topicName = do
  currentPartition <- newIORef 0
  let t = Topic topicName 5 currentPartition
  k <- newKafka defaultKafka
  pure (t, either (const Nothing) Just k)

heartbeats ::
     Kafka
  -> Topic
  -> GroupMember
  -> GenerationId
  -> TVar Bool
  -> String
  -> IO ()
heartbeats kafka top member genId interrupt name = do
  wait <- registerDelay 30000000
  void $ heartbeat kafka member genId
  resp <- getHeartbeatResponse kafka wait
  threadDelay 1000000
  case resp of
    Right (Right _) -> do
      putStrLn ("heartbeat (" <> name <> ")")
      halt <- atomically $ readTVar interrupt
      when (not halt) $ do
        heartbeats kafka top member genId interrupt name
    e -> do
      putStrLn "Failed to receive heartbeat response"

initGroupConsumer ::
     Kafka
  -> Topic
  -> GroupMember
  -> IO (GenerationId, GroupMember, [Member])
initGroupConsumer kafka top member@(GroupMember groupName _) = do
  wait <- registerDelay 30000000
  ex <- joinGroup kafka top member
  when (isLeft ex) (fail "Encountered network exception trying to join group")
  getJoinGroupResponse kafka wait >>= \case
    Right (Right jgr) -> do
      print jgr
      let memId = Just (fromByteString (memberId jgr))
      let assignment = GroupMember groupName memId
      void $ joinGroup kafka top assignment
      getJoinGroupResponse kafka wait >>= \case
        Right (Right jgr2) -> do
          print jgr2
          let genId = GenerationId (generationId jgr2)
          pure (genId, assignment, members jgr2)
        Right (Left e) -> do
          fail ("Failed parsing join group response: " <> show e)
        Left e -> do
          fail ("Encountered network exception trying to join group: " <> show e)
    Right (Left e) -> do
      fail ("Failed parsing join group response: " <> show e)
    Left e -> do
      fail ("Encountered network exception trying to join group: " <> show e)

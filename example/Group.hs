{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Monad
import Data.Either (isLeft)
import Data.IORef
import Data.Primitive.ByteArray (ByteArray)
import GHC.Conc

import Kafka
import Kafka.Common
import Kafka.Heartbeat.Response
import Kafka.JoinGroup.Response
import Kafka.SyncGroup.Response

main :: IO ()
main = do
  let name = fromByteString "example-consumer-group-1"
  (t, kafka) <- setup name
  case kafka of
    Nothing -> putStrLn "Failed to connect to kafka"
    Just k -> do
      let member = GroupMember name Nothing
      (genId, newMember) <- initGroupConsumer k t member
      interrupt <- registerDelay 5000000
      case newMember of
        GroupMember _ (Just memberName) -> do
          void $ syncGroup k newMember genId
            [ MemberAssignment memberName
                [TopicAssignment (fromByteString "test") [0]]
            ]
          registerDelay 3000000 >>= getSyncGroupResponse k >>= print
          heartbeats k t newMember genId interrupt
        _ -> do
          putStrLn "The server didn't assign a member id"

setup :: ByteArray -> IO (Topic, Maybe Kafka)
setup topicName = do
  currentPartition <- newIORef 0
  let t = Topic topicName 1 currentPartition
  k <- newKafka defaultKafka
  pure (t, either (const Nothing) Just k)

heartbeats ::
     Kafka
  -> Topic
  -> GroupMember
  -> GenerationId
  -> TVar Bool
  -> IO ()
heartbeats kafka top member genId interrupt = do
  wait <- registerDelay 2000000
  void $ heartbeat kafka member genId
  resp <- getHeartbeatResponse kafka wait
  threadDelay 1000000
  case resp of
    Right (Right r) -> do
      print r
      halt <- atomically $ readTVar interrupt
      when (not halt) $ do
        heartbeats kafka top member genId interrupt
    e -> do
      print e

initGroupConsumer ::
     Kafka
  -> Topic
  -> GroupMember
  -> IO (GenerationId, GroupMember)
initGroupConsumer kafka top member@(GroupMember groupName _) = do
  wait <- registerDelay 5000000
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
          pure (genId, assignment)
        Right (Left e) -> do
          fail ("Failed parsing join group response: " <> show e)
        Left e -> do
          fail ("Encountered network exception trying to join group: " <> show e)
    Right (Left e) -> do
      fail ("Failed parsing join group response: " <> show e)
    Left e -> do
      fail ("Encountered network exception trying to join group: " <> show e)

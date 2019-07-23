{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Monad
import Data.Either (isLeft)
import Data.Foldable
import Data.IORef
import Data.Primitive.ByteArray (ByteArray)
import GHC.Conc
import System.IO.Unsafe (unsafePerformIO)

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.Heartbeat.Response
import Kafka.JoinGroup.Response
import Kafka.SyncGroup.Response (SyncGroupResponse, SyncTopicAssignment)

import qualified Kafka.SyncGroup.Response as S
import qualified Kafka.ListOffsets.Response as L

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
  forkConsumer "1"
  forkConsumer "2"
  forkConsumer "3"
  waitForChildren

forkConsumer :: String -> IO ()
forkConsumer name = do
  mvar <- newEmptyMVar
  childs <- takeMVar children
  putMVar children (mvar:childs)
  void $ forkFinally (consumer name) (\_ -> putMVar mvar ())

consumer :: String -> IO ()
consumer name = do
  (t@(Topic topicName _ _), kafka) <- setup groupName 5
  case kafka of
    Nothing -> putStrLn "Failed to connect to kafka"
    Just k -> do
      let member = GroupMember groupName Nothing
      (genId, newMember, allMembers) <-
        initGroupConsumer k (TopicName topicName) member
      when (not (null allMembers)) $ do
        putStrLn (name <> " is the leader.")
      runConsumer t k name genId newMember allMembers

thirtySeconds :: Int
thirtySeconds = 30000000

assignMembers :: Int -> Topic -> [Member] -> [MemberAssignment]
assignMembers memberCount top groupMembers =
  fmap
    (assignMember memberCount top)
    (zip groupMembers [0..])

assignMember :: Int -> Topic -> (Member, Int) -> MemberAssignment
assignMember memberCount top (member, i) =
  MemberAssignment (fromByteString $ groupMemberId member)
    [TopicAssignment
      topicName
      (fromIntegral <$> assignPartitions memberCount i partitionCount)]
  where
    Topic topicName partitionCount _ = top

assignPartitions :: Int -> Int -> Int -> [Int]
assignPartitions memberCount i partitionCount =
  filter
    (\n -> mod (n + i) memberCount == 0)
    [0..partitionCount-1]

runConsumer ::
     Topic
  -> Kafka
  -> String
  -> GenerationId
  -> GroupMember
  -> [Member]
  -> IO ()
runConsumer t k name genId newMember allMembers = do
  void $ syncGroup k newMember genId (assignMembers (length allMembers) t allMembers)
  syncInterrupt <- registerDelay thirtySeconds
  resp <- S.getSyncGroupResponse k syncInterrupt
  case resp of
    Right (Right sgr) -> do
      reportPartitions name sgr
      void $ forkIO $
        withDefaultKafka $ \k' -> do
          let assigns = S.partitionAssignments <$> S.memberAssignment sgr
          case assigns of
            Just as -> consumeOn k' as
            Nothing -> putStrLn "Failed to receive an assignment"
      heartbeatInterrupt <- registerDelay thirtySeconds
      heartbeats k t newMember genId heartbeatInterrupt name
    Right (Left parseError) -> do
      print parseError
    Left networkError -> do
      print networkError

consumeOn ::
     Kafka
  -> [SyncTopicAssignment]
  -> IO ()
consumeOn k assignments =
  for_ assignments $ \a -> do
    let topicName = TopicName (fromByteString (S.syncAssignedTopic a))
        partitionIndices = S.syncAssignedPartitions a
    void $ listOffsets k topicName partitionIndices
    offsets <- L.getListOffsetsResponse k =<< registerDelay thirtySeconds
    case offsets of
      Right (Right offs) -> do
        case L.responses offs of
          [topicResponse] -> do
            let partitionOffsets =
                  fmap
                    (\r -> PartitionOffset (L.partition r) (L.offset r))
                    (L.partitionResponses topicResponse)
            void $ fetch k topicName thirtySeconds partitionOffsets
            resp <- getFetchResponse k =<< registerDelay thirtySeconds
            case resp of
              Right (Right r) -> do
                print r
                threadDelay 1000000
                consumeOn k assignments
              err -> do
                print err
          _ -> putStrLn "Got unexpected number of topic responses"
      err -> do
        print err

reportPartitions :: String -> SyncGroupResponse -> IO ()
reportPartitions name sgr = putStrLn
  (name <> ": my assigned partitions are " <> show (S.memberAssignment sgr))

setup :: ByteArray -> Int -> IO (Topic, Maybe Kafka)
setup topicName partitionCount = do
  currentPartition <- newIORef 0
  let t = Topic topicName partitionCount currentPartition
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
  wait <- registerDelay thirtySeconds
  void $ heartbeat kafka member genId
  resp <- getHeartbeatResponse kafka wait
  threadDelay 1000000
  case resp of
    Right (Right _) -> do
      halt <- atomically $ readTVar interrupt
      when (not halt) $ do
        heartbeats kafka top member genId interrupt name
    _ -> do
      putStrLn "Failed to receive heartbeat response"

initGroupConsumer ::
     Kafka
  -> TopicName
  -> GroupMember
  -> IO (GenerationId, GroupMember, [Member])
initGroupConsumer kafka top member@(GroupMember name _) = do
  wait <- registerDelay thirtySeconds
  ex <- joinGroup kafka top member
  when (isLeft ex) (fail "Encountered network exception trying to join group")
  getJoinGroupResponse kafka wait >>= \case
    Right (Right jgr) -> do
      print jgr
      let memId = Just (fromByteString (memberId jgr))
      let assignment = GroupMember name memId
      void $ joinGroup kafka top assignment
      getJoinGroupResponse kafka wait >>= \case
        Right (Right jgr2) -> do
          print jgr2
          let genId = GenerationId (generationId jgr2)
          pure (genId, assignment, members jgr2)
        Right (Left e) -> fail
          ("Failed parsing join group response: " <> show e)
        Left e -> fail
          ("Encountered network exception trying to join group: " <> show e)
    Right (Left e) -> fail
      ("Failed parsing join group response: " <> show e)
    Left e -> fail
      ("Encountered network exception trying to join group: " <> show e)

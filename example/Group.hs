{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Monad
import Data.Either (isLeft)
import Data.Foldable
import Data.Function
import Data.Int
import Data.IORef
import Data.Primitive.ByteArray (ByteArray)
import GHC.Conc
import System.IO.Unsafe (unsafePerformIO)

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.Heartbeat.Response
import Kafka.JoinGroup.Response (Member, getJoinGroupResponse)
import Kafka.LeaveGroup.Response
import Kafka.SyncGroup.Response (SyncGroupResponse, SyncTopicAssignment)

import qualified Kafka.JoinGroup.Response as J
import qualified Kafka.OffsetCommit.Response as C
import qualified Kafka.OffsetFetch.Response as O
import qualified Kafka.SyncGroup.Response as S

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
  (t, kafka) <- setup groupName 5
  case kafka of
    Nothing -> putStrLn "Failed to connect to kafka"
    Just k -> do
      let member = GroupMember groupName Nothing
      consume k t member name

thirtySeconds :: Int
thirtySeconds = 30000000

assignMembers :: Int -> Topic -> [Member] -> [MemberAssignment]
assignMembers memberCount top groupMembers =
  fmap
    (assignMember memberCount top)
    (zip groupMembers [0..])

assignMember :: Int -> Topic -> (Member, Int) -> MemberAssignment
assignMember memberCount top (member, i) =
  MemberAssignment (fromByteString $ J.groupMemberId member)
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

consumeOn ::
     Kafka
  -> GroupMember
  -> GenerationId
  -> [SyncTopicAssignment]
  -> IO ()
consumeOn k member genId assignments =
  for_ assignments $ \a -> do
    let topicName = TopicName (fromByteString (S.syncAssignedTopic a))
        partitionIndices = S.syncAssignedPartitions a
        initialOffsets = map (\ix -> PartitionOffset ix 0) partitionIndices
    go topicName partitionIndices initialOffsets
  where
  go topicName partitionIndices currentOffsets = do
    void $ offsetFetch k member topicName partitionIndices
    offsets <- O.getOffsetFetchResponse k =<< registerDelay thirtySeconds
    print offsets
    case offsets of
      Right (Right offs) -> do
        case O.topics offs of
          [topicResponse] -> do
            let partitions = O.offsetFetchPartitions topicResponse
                partitionOffsets =
                  fmap
                    (\r -> PartitionOffset
                      (O.offsetFetchPartitionIndex r)
                      (O.offsetFetchOffset r))
                    partitions
            void $ fetch k topicName thirtySeconds partitionOffsets
            resp <- getFetchResponse k =<< registerDelay thirtySeconds
            case resp of
              Right (Right r) -> do
                print r
                let newOffsets = updateOffsets topicName currentOffsets r
                putStrLn ("Updating offsets to " <> show newOffsets)
                threadDelay 1000000
                commitOffsets k topicName newOffsets member genId
                go topicName partitionIndices newOffsets
              err -> do
                print err
                putStrLn "failed to obtain fetch response"
          _ -> fail "Got unexpected number of topic responses"
      _ -> do
        fail "failed to obtain offsetfetch response"

commitOffsets ::
     Kafka
  -> TopicName
  -> [PartitionOffset]
  -> GroupMember
  -> GenerationId
  -> IO ()
commitOffsets k topicName offs member genId = do
  void $ offsetCommit k topicName offs member genId
  commitResponse <- C.getOffsetCommitResponse k =<< registerDelay thirtySeconds
  case commitResponse of
    Right (Right _) -> do
      putStrLn "successfully committed new offset"
    err -> do
      print err
      fail "failed to commit new offset"

nextOffset :: [RecordBatch] -> Int64
nextOffset batches =
  let lastBatch = maximumBy (compare `on` baseOffset) batches
  in  baseOffset lastBatch + fromIntegral (lastOffsetDelta lastBatch) + 1

updateOffsets :: TopicName -> [PartitionOffset] -> FetchResponse -> [PartitionOffset]
updateOffsets (TopicName topicName) xs r =
  let
    topicResps = filter
      (\x -> fetchResponseTopic x == toByteString topicName)
      (responses r)
    partitionResps = concatMap partitionResponses topicResps
    getPartitionResponse pid =
      find
        (\resp -> pid == partition (partitionHeader resp))
        partitionResps
  in
    fmap
      (\(PartitionOffset pid offs) ->
        case getPartitionResponse pid of
          Just res ->
            PartitionOffset pid (highWatermark (partitionHeader res))
          _ ->
            PartitionOffset pid offs)
      xs

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
  putStrLn (name <> ": " <> show resp)
  threadDelay 1000000
  case resp of
    Right (Right _) -> do
      halt <- atomically $ readTVar interrupt
      when (not halt) $ do
        heartbeats kafka top member genId interrupt name
    _ -> do
      putStrLn "Failed to receive heartbeat response"

consume ::
     Kafka
  -> Topic
  -> GroupMember
  -> String
  -> IO ()
consume kafka top@(Topic n _ _) member name = do
  let topicName = TopicName n
  (genId, me, members) <- joinG kafka topicName member
  assignment <- sync kafka top me members genId name
  putStrLn "Starting consumption"
  void $ forkIO $
    withDefaultKafka $ \k' -> do
      case assignment of
        Just as -> consumeOn k' me genId as
        Nothing -> putStrLn "Failed to receive an assignment"
  putStrLn "Starting heartbeats"
  heartbeatInterrupt <- registerDelay thirtySeconds
  heartbeats kafka top me genId heartbeatInterrupt name
  putStrLn "Leaving group"
  void $ leaveGroup kafka me
  print =<< getLeaveGroupResponse kafka =<< registerDelay thirtySeconds

sync ::
     Kafka
  -> Topic
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> String
  -> IO (Maybe [SyncTopicAssignment])
sync kafka top@(Topic n _ _) member members genId name = do
  let topicName = TopicName n
  let assignments = assignMembers (length members) top members
  ex <- syncGroup kafka member genId assignments
  when
    (isLeft ex)
    (fail ("Encountered network exception sending sync group request: "
      <> show ex))
  wait <- registerDelay thirtySeconds
  putStrLn "Syncing"
  S.getSyncGroupResponse kafka wait >>= \case
    Right (Right sgr) | S.errorCode sgr == 79 || S.errorCode sgr == 25 || S.errorCode sgr == 27 -> do
      putStrLn "Rejoining group"
      print sgr
      threadDelay 1000000
      (newGenId, newMember, newMembers) <- joinG kafka topicName member
      sync kafka top newMember newMembers newGenId name
    Right (Right sgr) | S.errorCode sgr == 0 -> do
      print sgr
      reportPartitions name sgr
      pure (S.partitionAssignments <$> S.memberAssignment sgr)
    Right (Right sgr) -> do
      print sgr
      fail ("Unexpected error from kafka during sync")
    Right (Left parseError) -> fail (show parseError)
    Left networkError -> fail (show networkError)

joinG ::
     Kafka
  -> TopicName
  -> GroupMember
  -> IO (GenerationId, GroupMember, [Member])
joinG kafka top member@(GroupMember name _) = do
  ex <- joinGroup kafka top member
  when
    (isLeft ex)
    (fail ("Encountered network exception sending join group request: "
      <> show ex))
  wait <- registerDelay thirtySeconds
  getJoinGroupResponse kafka wait >>= \case
    Right (Right jgr) | J.errorCode jgr == 79 -> do
      print jgr
      threadDelay 1000000
      let memId = Just (fromByteString (J.memberId jgr))
      let assignment = GroupMember name memId
      joinG kafka top assignment
    Right (Right jgr) | J.errorCode jgr == 0 -> do
      print jgr
      let genId = GenerationId (J.generationId jgr)
      let memId = Just (fromByteString (J.memberId jgr))
      let assignment = GroupMember name memId
      pure (genId, assignment, J.members jgr)
    Right (Right jgr) -> do
      print jgr
      fail ("Unexpected error from kafka during join")
    Right (Left e) -> fail
      ("Failed parsing join group response: " <> show e)
    Left e -> fail
      ("Encountered network exception trying to join group: " <> show e)

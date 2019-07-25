{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent
import Control.Monad
import Data.Either (isLeft)
import Data.Foldable
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
import qualified Kafka.ListOffsets.Response as L
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
    initialOffsets <- getInitialOffsets k topicName partitionIndices
    print initialOffsets
    case initialOffsets of
      Just ios -> consumerLoop k member genId topicName partitionIndices ios
      Nothing -> fail "Failed to get valid offsets for this topic"
  where

consumerLoop ::
     Kafka
  -> GroupMember
  -> GenerationId
  -> TopicName
  -> [Int32]
  -> [PartitionOffset]
  -> IO ()
consumerLoop k member genId topicName partitionIndices currentOffsets = do
  void $ fetch k topicName thirtySeconds currentOffsets
  resp <- getFetchResponse k =<< registerDelay thirtySeconds
  case resp of
    Right (Right r) -> do
      print r
      commitOffsets k topicName currentOffsets member genId
      void $ offsetFetch k member topicName partitionIndices
      offsets <- O.getOffsetFetchResponse k =<< registerDelay thirtySeconds
      print offsets
      case offsets of
        Right (Right offs) -> do
          case O.topics offs of
            [topicResponse] -> do
              let
                partitions = O.offsetFetchPartitions topicResponse
                partitionOffsets =
                  fmap
                    (\part -> PartitionOffset
                      (O.offsetFetchPartitionIndex part)
                      (O.offsetFetchOffset part))
                    partitions
                newOffsets = updateOffsets topicName partitionOffsets currentOffsets r
              putStrLn ("Updating offsets to " <> show newOffsets)
              threadDelay 1000000
              consumerLoop k member genId topicName partitionIndices newOffsets
            _ -> fail "Got unexpected number of topic responses"
        _ -> do
          fail "failed to obtain offsetfetch response"
    err -> do
      print err
      putStrLn "failed to obtain fetch response"

getInitialOffsets ::
     Kafka
  -> TopicName
  -> [Int32]
  -> IO (Maybe [PartitionOffset])
getInitialOffsets k t@(TopicName topicName) indices = do
  void $ listOffsets k t indices
  resp <- L.getListOffsetsResponse k =<< registerDelay thirtySeconds
  case resp of
    Right (Right lor) -> do
      let topics = L.responses lor
          thisTopic = find (\top -> L.topic top == toByteString topicName) topics
      pure $
        fmap
          (map (\pr ->
              PartitionOffset (L.partition pr) (L.offset pr))
            . L.partitionResponses)
          thisTopic
    _ -> pure Nothing

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

updateOffsets ::
     TopicName
  -> [PartitionOffset]
  -> [PartitionOffset]
  -> FetchResponse
  -> [PartitionOffset]
updateOffsets (TopicName topicName) current valid r =
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
        if offs == -1 then
          case find (\v -> partitionIndex v == pid) valid of
            Just v -> PartitionOffset pid (partitionOffset v)
            Nothing -> PartitionOffset pid (-1)
        else
          case getPartitionResponse pid of
            Just res ->
              PartitionOffset pid (highWatermark (partitionHeader res))
            _ ->
              PartitionOffset pid offs)
      current

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
  (newGenId, assignment) <- sync kafka top me members genId name
  putStrLn "Starting consumption"
  void $ forkIO $
    withDefaultKafka $ \k' -> do
      case assignment of
        Just as -> consumeOn k' me genId as
        Nothing -> putStrLn "Failed to receive an assignment"
  putStrLn "Starting heartbeats"
  heartbeatInterrupt <- registerDelay thirtySeconds
  heartbeats kafka top me newGenId heartbeatInterrupt name
  putStrLn "Leaving group"
  void $ leaveGroup kafka me
  print =<< getLeaveGroupResponse kafka =<< registerDelay thirtySeconds

noError :: Int16
noError = 0

errorUnknownMemberId :: Int16
errorUnknownMemberId = 25

errorRebalanceInProgress :: Int16
errorRebalanceInProgress = 27

errorMemberIdRequired :: Int16
errorMemberIdRequired = 79

sync ::
     Kafka
  -> Topic
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> String
  -> IO (GenerationId, Maybe [SyncTopicAssignment])
sync kafka top@(Topic n _ _) member members genId name = do
  let topicName = TopicName n
      assignments = assignMembers (length members) top members
      syncErrors = [errorUnknownMemberId, errorRebalanceInProgress, errorMemberIdRequired]
  ex <- syncGroup kafka member genId assignments
  when
    (isLeft ex)
    (fail ("Encountered network exception sending sync group request: "
      <> show ex))
  wait <- registerDelay thirtySeconds
  putStrLn "Syncing"
  S.getSyncGroupResponse kafka wait >>= \case
    Right (Right sgr) | S.errorCode sgr `elem` syncErrors -> do
      putStrLn "Rejoining group"
      print sgr
      threadDelay 1000000
      (newGenId, newMember, newMembers) <- joinG kafka topicName member
      sync kafka top newMember newMembers newGenId name
    Right (Right sgr) | S.errorCode sgr == noError -> do
      print sgr
      reportPartitions name sgr
      pure (genId, S.partitionAssignments <$> S.memberAssignment sgr)
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
    Right (Right jgr) | J.errorCode jgr == errorMemberIdRequired -> do
      print jgr
      threadDelay 1000000
      let memId = Just (fromByteString (J.memberId jgr))
      let assignment = GroupMember name memId
      joinG kafka top assignment
    Right (Right jgr) | J.errorCode jgr == noError -> do
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

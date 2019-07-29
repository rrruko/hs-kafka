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
  threadDelay 20000000
  forkConsumer "4"
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
      consumerSession k t member name

thirtySeconds :: Int
thirtySeconds = 30000000

consumerSession ::
     Kafka
  -> Topic
  -> GroupMember
  -> String
  -> IO ()
consumerSession kafka top oldMe name = do
  let topicName = getTopicName top
  (genId, me, members) <- joinG kafka topicName oldMe
  (newGenId, assigns) <- sync kafka top me members genId name
  case assigns of
    Nothing -> pure ()
    Just as -> do
      case partitionsForTopic topicName as of
        Just indices -> do
          offs <- getInitialOffsets kafka topicName indices
          print offs
          case offs of
            Just initialOffsets -> forever $
              loop kafka top me initialOffsets indices newGenId name
            Nothing -> fail "The topic was not present in the listed offset set"
        Nothing -> fail "The topic was not present in the assignment set"
  putStrLn "Leaving group"
  void $ leaveGroup kafka me
  wait <- registerDelay thirtySeconds
  leaveResp <- getLeaveGroupResponse kafka wait
  print leaveResp

partitionsForTopic ::
     TopicName
  -> [SyncTopicAssignment]
  -> Maybe [Int32]
partitionsForTopic (TopicName n) assigns =
  S.syncAssignedPartitions
  <$> find (\a -> S.syncAssignedTopic a == toByteString n) assigns

loop ::
     Kafka
  -> Topic
  -> GroupMember
  -> [PartitionOffset]
  -> [Int32]
  -> GenerationId
  -> String
  -> IO ()
loop kafka top member offsets indices genId name = do
  let topicName = getTopicName top
  rebalanceInterrupt <- registerDelay 10000000
  fetchResp <- getMessages kafka top offsets rebalanceInterrupt
  print fetchResp
  case fetchResp of
    Right (Right r) -> do
      newOffsets <- updateOffsets' kafka topicName member indices r
      commitOffsets kafka topicName offsets member genId
      wait <- registerDelay thirtySeconds
      void $ heartbeat kafka member genId
      resp <- getHeartbeatResponse kafka wait
      putStrLn (name <> ": " <> show resp)
      case (fmap . fmap) heartbeatErrorCode resp of
        Right (Right 0)  -> loop kafka top member newOffsets indices genId name
        Right (Right 27) -> consumerSession kafka top member name
        Right (Right e)  ->
          fail ("Unexpected error code from heartbeat: " <> show e)
        Right (Left parseError) -> do
          fail ("Failed to parse, expected heartbeat (" <> parseError <> ")")
        Left netError -> do
          fail ("Unexpected network error (" <> show netError <> ")")
    Right (Left parseError) -> do
      fail ("Couldn't parse fetch response: " <> show parseError)
    Left networkError -> do
      putStrLn ("Encountered network error: " <> show networkError)
      loop kafka top member offsets indices genId name

getMessages ::
     Kafka
  -> Topic
  -> [PartitionOffset]
  -> TVar Bool
  -> IO (Either KafkaException (Either String FetchResponse))
getMessages kafka top offsets interrupt = do
  void $ fetch kafka (getTopicName top) 5000000 offsets
  resp <- getFetchResponse kafka interrupt
  pure resp

updateOffsets' ::
     Kafka
  -> TopicName
  -> GroupMember
  -> [Int32]
  -> FetchResponse
  -> IO [PartitionOffset]
updateOffsets' k topicName member partitionIndices r = do
  void $ offsetFetch k member topicName partitionIndices
  offsets <- O.getOffsetFetchResponse k =<< registerDelay thirtySeconds
  print offsets
  case offsets of
    Right (Right offs) -> do
      case O.topics offs of
        [topicResponse] -> do
          let
            partitions = O.offsetFetchPartitions topicResponse
            fetchedOffsets =
              fmap
                (\part -> PartitionOffset
                  (O.offsetFetchPartitionIndex part)
                  (O.offsetFetchOffset part))
                partitions
          pure (updateOffsets topicName fetchedOffsets r)
        _ -> fail "Got unexpected number of topic responses"
    _ -> do
      fail "failed to obtain offsetfetch response"

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
  -> FetchResponse
  -> [PartitionOffset]
updateOffsets (TopicName topicName) current r =
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
      current

reportPartitions :: String -> SyncGroupResponse -> IO ()
reportPartitions name sgr = putStrLn
  (name <> ": my assigned partitions are " <> show (S.memberAssignment sgr))

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

setup :: ByteArray -> Int -> IO (Topic, Maybe Kafka)
setup topicName partitionCount = do
  currentPartition <- newIORef 0
  let t = Topic topicName partitionCount currentPartition
  k <- newKafka defaultKafka
  pure (t, either (const Nothing) Just k)

noError :: Int16
noError = 0

errorUnknownMemberId :: Int16
errorUnknownMemberId = 25

errorRebalanceInProgress :: Int16
errorRebalanceInProgress = 27

errorMemberIdRequired :: Int16
errorMemberIdRequired = 79

expectedSyncErrors :: [Int16]
expectedSyncErrors =
  [ errorUnknownMemberId
  , errorRebalanceInProgress
  , errorMemberIdRequired
  ]

sync ::
     Kafka
  -> Topic
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> String
  -> IO (GenerationId, Maybe [SyncTopicAssignment])
sync kafka top member members genId name = do
  let topicName = getTopicName top
      assignments = assignMembers (length members) top members
  ex <- syncGroup kafka member genId assignments
  when
    (isLeft ex)
    (fail ("Encountered network exception sending sync group request: "
      <> show ex))
  wait <- registerDelay thirtySeconds
  putStrLn "Syncing"
  S.getSyncGroupResponse kafka wait >>= \case
    Right (Right sgr) | S.errorCode sgr `elem` expectedSyncErrors -> do
      putStrLn "Rejoining group"
      print sgr
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

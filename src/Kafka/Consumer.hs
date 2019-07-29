{-# LANGUAGE LambdaCase #-}

module Kafka.Consumer
  ( consumerSession
  , merge
  ) where

import Control.Monad
import Data.ByteString (ByteString)
import Data.Either
import Data.Foldable
import Data.Int
import Data.IntMap (IntMap)
import Data.Maybe
import GHC.Conc

import qualified Data.ByteString.Char8 as B
import qualified Data.IntMap as IM

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.Heartbeat.Response
import Kafka.JoinGroup.Response (Member, getJoinGroupResponse)
import Kafka.LeaveGroup.Response
import Kafka.ListOffsets.Response (ListOffsetsResponse)
import Kafka.OffsetFetch.Response (OffsetFetchResponse)
import Kafka.SyncGroup.Response (SyncGroupResponse, SyncTopicAssignment)

import qualified Kafka.Fetch.Response as F
import qualified Kafka.JoinGroup.Response as J
import qualified Kafka.ListOffsets.Response as L
import qualified Kafka.OffsetCommit.Response as C
import qualified Kafka.OffsetFetch.Response as O
import qualified Kafka.SyncGroup.Response as S

-- | This module provides a high-level interface to the Kafka API for
-- consumers by wrapping the low-level request and response type modules.

-- | Get the earliest offsets corresponding to unread messages on the topic.
-- The group may not have committed an offset on a given partition yet, so we
-- need to send a ListOffsets request to find out which offsets are even valid
-- on the topic and default to that offset if a committed offset is not
-- present.
getInitialOffsets ::
     Kafka
  -> GroupMember
  -> Topic
  -> [Int32]
  -> IO (Maybe (IntMap Int64))
getInitialOffsets kafka member topic indices = do
  let Topic name _ _ = topic
  let topicName = TopicName name
  let name' = toByteString name
  void $ listOffsets kafka topicName indices
  listedOffs <- L.getListOffsetsResponse kafka =<< registerDelay defaultTimeout
  void $ offsetFetch kafka member topicName indices
  committedOffs <- O.getOffsetFetchResponse kafka =<< registerDelay defaultTimeout
  case (listedOffs, committedOffs) of
    (Right (Right lor), Right (Right ofr)) ->
      case (listOffsetsMap name' lor, fetchOffsetsMap name' ofr) of
        (Just lm, Just om) -> do
          let res = merge lm om
          print res
          pure (Just res)
        _ -> pure Nothing
    _ -> pure Nothing

toOffsetList :: IntMap Int64 -> [PartitionOffset]
toOffsetList = map (\(k, v) -> PartitionOffset (fromIntegral k) v) . IM.toList

listOffsetsMap :: ByteString -> ListOffsetsResponse -> Maybe (IntMap Int64)
listOffsetsMap topicName lor = do
  thisTopic <- find (\t -> L.topic t == topicName) (L.responses lor)
  pure . IM.fromList $
    map
      (\pr -> (fromIntegral (L.partition pr), L.offset pr))
      (L.partitionResponses thisTopic)

fetchOffsetsMap :: ByteString -> OffsetFetchResponse -> Maybe (IntMap Int64)
fetchOffsetsMap topicName ofr = do
  thisTopic <- find (\t -> O.offsetFetchTopic t == topicName) (O.topics ofr)
  pure . IM.fromList $
    map
      (\pr ->
        (fromIntegral (O.offsetFetchPartitionIndex pr)
        , O.offsetFetchOffset pr))
      (O.offsetFetchPartitions thisTopic)

merge :: IntMap Int64 -> IntMap Int64 -> IntMap Int64
merge lor ofr =
  IM.mergeWithKey
    (\_ l o -> Just $ if o < 0 then l else o)
    id
    id
    lor
    ofr

defaultTimeout :: Int
defaultTimeout = 30000000

fiveSeconds :: Int
fiveSeconds = 5000000

updateOffsets ::
     TopicName
  -> IntMap Int64
  -> FetchResponse
  -> IntMap Int64
updateOffsets (TopicName topicName) current r =
  let
    topicResps = filter
      (\x -> F.fetchResponseTopic x == toByteString topicName)
      (F.responses r)
    partitionResps = concatMap partitionResponses topicResps
    getPartitionResponse pid =
      find
        (\resp -> pid == partition (partitionHeader resp))
        partitionResps
  in
    IM.mapWithKey
      (\pid offs ->
        maybe
          offs
          (highWatermark . partitionHeader)
          (getPartitionResponse (fromIntegral pid)))
      current

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
          offs <- getInitialOffsets kafka me top indices
          print offs
          case offs of
            Just initialOffsets -> forever $
              loop kafka top me initialOffsets indices newGenId name
            Nothing -> fail "The topic was not present in the listed offset set"
        Nothing -> fail "The topic was not present in the assignment set"
  putStrLn "Leaving group"
  void $ leaveGroup kafka me
  wait <- registerDelay defaultTimeout
  leaveResp <- getLeaveGroupResponse kafka wait
  pure ()
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
  -> IntMap Int64
  -> [Int32]
  -> GenerationId
  -> String
  -> IO ()
loop kafka top member offsets indices genId name = do
  let topicName = getTopicName top
  rebalanceInterrupt <- registerDelay fiveSeconds
  fetchResp <- getMessages kafka top offsets rebalanceInterrupt
  print offsets
  case fetchResp of
    Right (Right r) -> do
      traverse_ B.putStrLn (fetchResponseContents r)
      newOffsets <- updateOffsets' kafka topicName member indices r
      commitOffsets kafka topicName newOffsets member genId
      wait <- registerDelay defaultTimeout
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
  -> IntMap Int64
  -> TVar Bool
  -> IO (Either KafkaException (Either String FetchResponse))
getMessages kafka top offsets interrupt = do
  void $ fetch kafka (getTopicName top) defaultTimeout (toOffsetList offsets)
  getFetchResponse kafka interrupt

updateOffsets' ::
     Kafka
  -> TopicName
  -> GroupMember
  -> [Int32]
  -> FetchResponse
  -> IO (IntMap Int64)
updateOffsets' k topicName member partitionIndices r = do
  void $ offsetFetch k member topicName partitionIndices
  offsets <- O.getOffsetFetchResponse k =<< registerDelay defaultTimeout
  print offsets
  case offsets of
    Right (Right offs) -> do
      case O.topics offs of
        [topicResponse] -> do
          let
            partitions = O.offsetFetchPartitions topicResponse
            fetchedOffsets = IM.fromList $
              fmap
                (\part ->
                  (fromIntegral $ O.offsetFetchPartitionIndex part
                  , O.offsetFetchOffset part))
                partitions
          pure (updateOffsets topicName fetchedOffsets r)
        _ -> fail "Got unexpected number of topic responses"
    _ -> do
      fail "failed to obtain offsetfetch response"

commitOffsets ::
     Kafka
  -> TopicName
  -> IntMap Int64
  -> GroupMember
  -> GenerationId
  -> IO ()
commitOffsets k topicName offs member genId = do
  void $ offsetCommit k topicName (toOffsetList offs) member genId
  commitResponse <- C.getOffsetCommitResponse k =<< registerDelay defaultTimeout
  case commitResponse of
    Right (Right _) -> do
      putStrLn ("successfully committed new offset (" <> show offs <> ")")
    err -> do
      print err
      fail "failed to commit new offset"

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
  [i, i + memberCount .. partitionCount-1]

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

fetchResponseContents :: FetchResponse -> [ByteString]
fetchResponseContents fetchResponse =
    mapMaybe recordValue
  . concatMap F.records
  . concat
  . mapMaybe F.recordSet
  . concatMap F.partitionResponses
  . F.responses
  $ fetchResponse

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
  wait <- registerDelay defaultTimeout
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
  wait <- registerDelay defaultTimeout
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

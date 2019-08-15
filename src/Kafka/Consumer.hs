{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Kafka.Consumer
  ( ConsumerSettings(..)
  , ConsumerState(..)
  , commitOffsets
  , runExceptT
  , runConsumer
  , getRecordSet
  , leave
  , merge
  , newConsumer
  , rejoin
  ) where

import Chronos
import Control.Concurrent.STM.TVar
import Control.Monad hiding (join)
import Control.Monad.Except hiding (join)
import Data.Coerce
import Data.Foldable
import Data.Primitive.ByteArray
import Data.Int
import Data.IntMap (IntMap)
import Data.Maybe

import qualified Data.IntMap as IM

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.JoinGroup.Response (Member, getJoinGroupResponse)
import Kafka.LeaveGroup.Response
import Kafka.ListOffsets.Response (ListOffsetsResponse)
import Kafka.SyncGroup.Response (SyncTopicAssignment)

import qualified Kafka.Fetch.Response as F
import qualified Kafka.JoinGroup.Response as J
import qualified Kafka.ListOffsets.Response as L
import qualified Kafka.Metadata.Response as M
import qualified Kafka.OffsetCommit.Response as C
import qualified Kafka.OffsetFetch.Response as O
import qualified Kafka.SyncGroup.Response as S

-- | This module provides a high-level interface to the Kafka API for
-- consumers by wrapping the low-level request and response type modules.
newtype Consumer a
  = Consumer { runConsumer :: ExceptT KafkaException IO a }
  deriving (Functor, Applicative, Monad)

tryParse :: Either KafkaException (Either String a) -> Either KafkaException a
tryParse = \case
  Right (Right parsed) -> Right parsed
  Right (Left parseError) -> Left (KafkaParseException parseError)
  Left networkError -> Left networkError

instance MonadIO Consumer where
  liftIO = Consumer . ExceptT . fmap Right

instance Semigroup a => Semigroup (Consumer a) where
  Consumer x <> Consumer y = Consumer $ do
    v <- x
    v' <- y
    pure (v <> v')

instance Monoid a => Monoid (Consumer a) where
  mempty = pure mempty

liftConsumer :: IO (Either KafkaException a) -> Consumer a
liftConsumer = Consumer . ExceptT

throwConsumer :: KafkaException -> Consumer a
throwConsumer = Consumer . throwError

data ConsumerSettings = ConsumerSettings
  { csGroupFetchStart :: !KafkaTimestamp -- ^ Where to start if the group is new
  , csMaxFetchBytes :: !Int32 -- ^ Maximum number of bytes to allow per response
  , csTopicName :: !TopicName -- ^ Topic to fetch on
  , csGroupName :: !ByteArray -- ^ Name of the group
  , defaultTimeout :: Int
  } deriving (Show)

data ConsumerState = ConsumerState
  { kafka :: Kafka
  , settings :: ConsumerSettings
  , member :: GroupMember
  , genId :: GenerationId
  , members :: [Member]
  , offsets :: IntMap Int64
  , partitionCount :: Int32
  , lastRequestTime :: Time
  , errorCode :: Int16
  } deriving (Show)

getListedOffsets :: [Int32] -> ConsumerState -> Consumer (IntMap Int64)
getListedOffsets allIndices (ConsumerState {..}) = do
  let ConsumerSettings {..} = settings
  liftConsumer $ listOffsets kafka csTopicName allIndices csGroupFetchStart
  listOffsetsTimeout <- liftIO (registerDelay defaultTimeout)
  listedOffs <- liftConsumer $ tryParse <$>
    L.getListOffsetsResponse kafka listOffsetsTimeout
  pure (listOffsetsMap listedOffs)

initializeOffsets ::
     [Int32]
  -> ConsumerState
  -> Consumer ConsumerState
initializeOffsets assignedPartitions state@(ConsumerState {..}) = do
  let ConsumerSettings {..} = settings
  initialOffs <- getListedOffsets assignedPartitions state
  latestOffs <- latestOffsets assignedPartitions state
  let validOffs = merge initialOffs latestOffs
  let newState = state { offsets = validOffs }
  commitOffsets newState
  pure newState

merge :: IntMap Int64 -> IntMap Int64 -> IntMap Int64
merge lor ofr = mergeId (\l o -> Just $ if o < 0 then l else o) lor ofr
  where
  mergeId f a b = IM.mergeWithKey (\_ left right -> f left right) id id a b

toOffsetList :: IntMap Int64 -> [PartitionOffset]
toOffsetList = map (\(k, v) -> PartitionOffset (fromIntegral k) v) . IM.toList

listOffsetsMap :: ListOffsetsResponse -> IntMap Int64
listOffsetsMap lor = IM.fromList $ map
  (\pr -> (fromIntegral (L.partition pr), L.offset pr))
  (concatMap L.partitionResponses $ L.responses lor)

updateOffsets :: TopicName -> IntMap Int64 -> FetchResponse -> IntMap Int64
updateOffsets topicName current r =
  IM.mapWithKey
    (\pid offs -> fromMaybe offs
      (F.partitionLastSeenOffset r name (fromIntegral pid)))
    current
  where
  name = toByteString (coerce topicName)

getPartitionCount :: Kafka -> TopicName -> Int -> Consumer Int32
getPartitionCount kafka topicName timeout = do
  _ <- liftConsumer $ metadata kafka topicName NeverCreate
  interrupt <- liftIO $ registerDelay timeout
  parts <- fmap (metadataPartitions topicName) $ liftConsumer $
    tryParse <$> M.getMetadataResponse kafka interrupt
  case parts of
    Just p -> pure p
    Nothing -> throwConsumer $
      KafkaException "Topic name not found in metadata request"

metadataPartitions :: TopicName -> M.MetadataResponse -> Maybe Int32
metadataPartitions topicName mdr =
  let tops = M.metadataTopics mdr
  in  case find ((== topicName) . coerce . fromByteString . M.mtName) tops of
        Just top -> Just (fromIntegral $ length (M.mtPartitions top))
        Nothing -> Nothing

newConsumer ::
     Kafka
  -> ConsumerSettings
  -> IO (Either KafkaException ConsumerState)
newConsumer kafka settings@(ConsumerSettings {..}) = runExceptT $ runConsumer $ do
  let initialMember = GroupMember csGroupName Nothing
  partitionCount <- getPartitionCount kafka csTopicName defaultTimeout
  (genId', me, members) <- join kafka csTopicName initialMember defaultTimeout
  (newGenId, assigns) <- sync kafka csTopicName partitionCount me members genId' defaultTimeout
  case partitionsForTopic csTopicName assigns of
    Just indices -> do
      let state = ConsumerState
            { settings = settings
            , genId = newGenId
            , kafka = kafka
            , member = me
            , members = members
            , offsets = mempty
            , partitionCount = partitionCount
            , lastRequestTime = epoch
            , errorCode = 0
            }
      initializeOffsets indices state
    Nothing -> throwConsumer $ KafkaException
      "The topic was not present in the assignment set"

-- | rejoin is called when the client receives a "rebalance in progress"
-- error code, triggered by another client joining or leaving the group.
-- It sends join and sync requests and receives a new member id, generation
-- id, and set of assigned topics.
rejoin ::
     ConsumerState
  -> Consumer ConsumerState
rejoin state@(ConsumerState {..}) = do
  let topicName = csTopicName settings
  (genId', newMember, members') <- join kafka topicName member (defaultTimeout settings)
  (newGenId, assigns) <- sync kafka topicName partitionCount newMember members' genId' (defaultTimeout settings)
  case partitionsForTopic topicName assigns of
    Just indices -> do
      newOffsets <- latestOffsets indices state
      pure
        (state
          { member = newMember
          , genId = newGenId
          , offsets = newOffsets
          })
    Nothing -> throwConsumer $ KafkaException
      "The topic was not present in the assignment set"

partitionsForTopic :: TopicName -> [SyncTopicAssignment] -> Maybe [Int32]
partitionsForTopic (TopicName n) assigns =
  S.syncAssignedPartitions
  <$> find (\a -> S.syncAssignedTopic a == toByteString n) assigns

leave :: ConsumerState -> Consumer ()
leave (ConsumerState {..}) = do
  liftConsumer $ leaveGroup kafka member
  timeout <- liftIO $ registerDelay (defaultTimeout settings)
  void $ liftConsumer $ tryParse <$> getLeaveGroupResponse kafka timeout

getRecordSet ::
     Int
  -> ConsumerState
  -> Consumer (FetchResponse, ConsumerState)
getRecordSet timeout state@(ConsumerState {..}) = do
  let ConsumerSettings {..} = settings
  liftConsumer $ fetch kafka csTopicName timeout offsetList csMaxFetchBytes
  interrupt <- liftIO $ registerDelay timeout
  fetchResp <- liftConsumer $ tryParse <$> getFetchResponse kafka interrupt
  let newState = state { offsets = updateOffsets csTopicName offsets fetchResp }
  pure (fetchResp, newState)
  where
  offsetList = toOffsetList offsets

latestOffsets :: [Int32] -> ConsumerState -> Consumer (IntMap Int64)
latestOffsets indices (ConsumerState {..}) = do
  liftConsumer $ offsetFetch kafka member (csTopicName settings) indices
  timeout <- liftIO $ registerDelay (defaultTimeout settings)
  offs <- liftConsumer $ tryParse <$> O.getOffsetFetchResponse kafka timeout
  pure (offsetFetchOffsets offs)

offsetFetchOffsets :: O.OffsetFetchResponse -> IntMap Int64
offsetFetchOffsets ofr = IM.fromList $ fmap
  (\part ->
    (fromIntegral $ O.offsetFetchPartitionIndex part
      , O.offsetFetchOffset part))
    (concatMap O.offsetFetchPartitions $ O.topics ofr)

commitOffsets ::
     ConsumerState
  -> Consumer ()
commitOffsets (ConsumerState {..}) = do
  let ConsumerSettings {..} = settings
  liftConsumer $ offsetCommit kafka csTopicName (toOffsetList offsets) member genId
  timeout <- liftIO $ registerDelay defaultTimeout
  void $ liftConsumer $ tryParse <$> C.getOffsetCommitResponse kafka timeout

assignMembers :: Int -> TopicName -> Int32 -> [Member] -> [MemberAssignment]
assignMembers memberCount topicName partitionCount groupMembers =
  fmap (assignMember memberCount topicName partitionCount) (zip groupMembers [0..])

assignMember :: Int -> TopicName -> Int32 -> (Member, Int) -> MemberAssignment
assignMember memberCount topicName partitionCount (member, i) =
  MemberAssignment
    memberName
    [TopicAssignment (coerce topicName) assignments]
  where
  memberName = fromByteString (J.groupMemberId member)
  assignments = fromIntegral <$>
    assignPartitions memberCount i partitionCount

assignPartitions :: Int -> Int -> Int32 -> [Int]
assignPartitions memberCount i partitionCount =
  [i, i + memberCount .. fromIntegral partitionCount-1]

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
  -> TopicName
  -> Int32
  -> GroupMember
  -> [Member]
  -> GenerationId
  -> Int
  -> Consumer (GenerationId, [SyncTopicAssignment])
sync kafka topicName partitionCount member members genId timeout = do
  let assignments = assignMembers (length members) topicName partitionCount members
  liftConsumer $ syncGroup kafka member genId assignments
  wait <- liftIO (registerDelay timeout)
  sgr <- liftConsumer $ tryParse <$> S.getSyncGroupResponse kafka wait
  if S.errorCode sgr `elem` expectedSyncErrors then do
    (newGenId, newMember, newMembers) <- join kafka topicName member timeout
    sync kafka topicName partitionCount newMember newMembers newGenId timeout
  else if S.errorCode sgr == noError then do
    let assigns = S.partitionAssignments <$> S.memberAssignment sgr
    pure (genId, fromMaybe [] assigns)
  else
    throwConsumer (KafkaUnexpectedErrorCodeException (S.errorCode sgr))

join ::
     Kafka
  -> TopicName
  -> GroupMember
  -> Int
  -> Consumer (GenerationId, GroupMember, [Member])
join kafka top member@(GroupMember name _) timeout = do
  liftConsumer $ joinGroup kafka top member
  wait <- liftIO (registerDelay timeout)
  jgr <- liftConsumer $ tryParse <$> getJoinGroupResponse kafka wait
  if J.errorCode jgr == errorMemberIdRequired then do
    let memId = Just (fromByteString (J.memberId jgr))
    let assignment = GroupMember name memId
    join kafka top assignment timeout
  else if J.errorCode jgr == noError then do
    let genId = GenerationId (J.generationId jgr)
    let memId = Just (fromByteString (J.memberId jgr))
    let assignment = GroupMember name memId
    pure (genId, assignment, J.members jgr)
  else
    throwConsumer (KafkaUnexpectedErrorCodeException (J.errorCode jgr))

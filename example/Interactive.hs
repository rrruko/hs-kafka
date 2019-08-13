{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.ByteString (ByteString)
import Data.Int
import Data.IORef
import Data.Primitive.ByteArray (ByteArray)
import Data.Primitive.Unlifted.Array
import GHC.Conc

import Kafka
import Kafka.Common
import Kafka.Fetch.Response
import Kafka.ListOffsets.Response
import Kafka.JoinGroup.Response
import Kafka.OffsetCommit.Response
import Kafka.OffsetFetch.Response
import Kafka.Produce.Response

main :: IO ()
main = pure ()

-- Politely ask Kafka to respond within 2 seconds
requestedWaitTime :: Int
requestedWaitTime = 2000000

-- Give up on waiting if we don't get a response in 5 seconds
giveUpTime :: Int
giveUpTime = 5000000

setup :: ByteArray -> IO (Topic, Maybe Kafka)
setup topicName = do
  currentPartition <- newIORef 0
  let t = Topic topicName 1 currentPartition
  k <- newKafka defaultKafka
  pure (t, either (const Nothing) Just k)

iProduce ::
     Kafka
  -> Topic
  -> [ByteString]
  -> IO (Either KafkaException (Either String ProduceResponse))
iProduce kafka top byteStrings = do
  wait <- registerDelay giveUpTime
  let msgs = unliftedArrayFromList $ fmap fromByteString byteStrings
  _ <- produce kafka top requestedWaitTime msgs
  getProduceResponse kafka wait

iFetch ::
     Kafka
  -> TopicName
  -> Int64
  -> IO (Either KafkaException (Either String FetchResponse))
iFetch kafka top offs = do
  wait <- registerDelay giveUpTime
  let maxBytes = 30 * 1000 * 1000
  _ <- fetch kafka top requestedWaitTime [PartitionOffset 0 offs] maxBytes
  getFetchResponse kafka wait

iListOffsets ::
     Kafka
  -> TopicName
  -> KafkaTimestamp
  -> IO (Either KafkaException (Either String ListOffsetsResponse))
iListOffsets kafka top time = do
  wait <- registerDelay giveUpTime
  _ <- listOffsets kafka top [0] time
  getListOffsetsResponse kafka wait

iJoinGroup ::
     Kafka
  -> TopicName
  -> GroupMember
  -> IO (Either KafkaException (Either String JoinGroupResponse))
iJoinGroup kafka top member = do
  wait <- registerDelay giveUpTime
  _ <- joinGroup kafka top member
  getJoinGroupResponse kafka wait

iOffsetCommit ::
     Kafka
  -> TopicName
  -> [PartitionOffset]
  -> GroupMember
  -> GenerationId
  -> IO (Either KafkaException (Either String OffsetCommitResponse))
iOffsetCommit kafka top offs member genId = do
  wait <- registerDelay giveUpTime
  _ <- offsetCommit kafka top offs member genId
  getOffsetCommitResponse kafka wait

iOffsetFetch ::
     Kafka
  -> TopicName
  -> GroupMember
  -> [Int32]
  -> IO (Either KafkaException (Either String OffsetFetchResponse))
iOffsetFetch kafka top member offs = do
  wait <- registerDelay giveUpTime
  _ <- offsetFetch kafka member top offs
  getOffsetFetchResponse kafka wait

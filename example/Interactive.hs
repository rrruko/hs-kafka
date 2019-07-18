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
  -> Topic
  -> Int64
  -> IO (Either KafkaException (Either String FetchResponse))
iFetch kafka top offs = do
  wait <- registerDelay giveUpTime
  _ <- fetch kafka top requestedWaitTime [Partition 0 offs]
  getFetchResponse kafka wait

iListOffsets ::
     Kafka
  -> Topic
  -> IO (Either KafkaException (Either String ListOffsetsResponse))
iListOffsets kafka top = do
  wait <- registerDelay giveUpTime
  _ <- listOffsets kafka top [0]
  getListOffsetsResponse kafka wait

iJoinGroup ::
     Kafka
  -> Topic
  -> GroupMember
  -> IO (Either KafkaException (Either String JoinGroupResponse))
iJoinGroup kafka top member = do
  wait <- registerDelay giveUpTime
  _ <- joinGroup kafka top member
  getJoinGroupResponse kafka wait

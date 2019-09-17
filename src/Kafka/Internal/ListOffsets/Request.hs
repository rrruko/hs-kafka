module Kafka.Internal.ListOffsets.Request
  ( listOffsetsRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

listOffsetsApiVersion :: Int16
listOffsetsApiVersion = 5

listOffsetsApiKey :: Int16
listOffsetsApiKey = 2

defaultReplicaId :: Int32
defaultReplicaId = -1

data IsolationLevel
  = ReadUncommitted
  | ReadCommitted

isolationLevel :: IsolationLevel -> Int8
isolationLevel ReadUncommitted = 0
isolationLevel ReadCommitted = 1

defaultIsolationLevel :: Int8
defaultIsolationLevel = isolationLevel ReadUncommitted

defaultCurrentLeaderEpoch :: Int32
defaultCurrentLeaderEpoch = -1

kafkaTimestamp :: KafkaTimestamp -> Int64
kafkaTimestamp Latest = -1
kafkaTimestamp Earliest = -2
kafkaTimestamp (At n) = n

listOffsetsRequest ::
     TopicName
  -> [Int32]
  -> KafkaTimestamp
  -> UnliftedArray ByteArray
listOffsetsRequest topic partitions timestamp =
  let
    minimumReqSize = 25
    partitionMessageSize = 16
    reqSize = fromIntegral $
        minimumReqSize
      + clientIdLength
      + topicNameSize
      + partitionMessageSize * partitionCount
    req = build $
      int32 reqSize
      -- common request headers
      <> int16 listOffsetsApiKey
      <> int16 listOffsetsApiVersion
      <> int32 correlationId
      <> string (fromByteString clientId) clientIdLength
      -- listoffsets request
      <> int32 defaultReplicaId
      <> int8 defaultIsolationLevel
      <> int32 1 -- number of following topics

      <> string topicName topicNameSize
      <> array
          ( map
            (\p -> int32 p
              <> int32 defaultCurrentLeaderEpoch
              <> int64 (kafkaTimestamp timestamp)
            )
            partitions
          )
          (fromIntegral partitionCount)
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 1 mempty
      writeUnliftedArray arr 0 req
      pure arr
  where
    TopicName topicName = topic
    topicNameSize = sizeofByteArray topicName
    partitionCount = length partitions

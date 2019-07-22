module Kafka.ListOffsets.Request
  ( listOffsetsRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Writer

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

data KafkaTimestamp
  = Latest
  | Earliest
  | At Int64

defaultTimestamp :: KafkaTimestamp
defaultTimestamp = Latest

kafkaTimestamp :: KafkaTimestamp -> Int64
kafkaTimestamp Latest = -1
kafkaTimestamp Earliest = -2
kafkaTimestamp (At n) = n

listOffsetsRequest ::
     TopicName
  -> [Int32]
  -> UnliftedArray ByteArray
listOffsetsRequest topic partitions =
  let
    minimumReqSize = 25
    partitionMessageSize = 16
    reqSize = fromIntegral $
        minimumReqSize
      + clientIdLength
      + topicNameSize
      + partitionMessageSize * partitionCount
    req = evaluate $ foldBuilder $
      [ build32 reqSize
      -- common request headers
      , build16 listOffsetsApiKey
      , build16 listOffsetsApiVersion
      , build32 correlationId
      , buildString (fromByteString clientId) clientIdLength
      -- listoffsets request
      , build32 defaultReplicaId
      , build8 defaultIsolationLevel
      , build32 1 -- number of following topics

      , buildString topicName topicNameSize
      , buildArray
          (map
            (\p -> foldBuilder
              [ build32 p
              , build32 defaultCurrentLeaderEpoch
              , build64 (kafkaTimestamp defaultTimestamp)
              ])
            partitions)
          (fromIntegral partitionCount)
      ]
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 1 mempty
      writeUnliftedArray arr 0 req
      pure arr
  where
    TopicName topicName = topic
    topicNameSize = sizeofByteArray topicName
    partitionCount = length partitions

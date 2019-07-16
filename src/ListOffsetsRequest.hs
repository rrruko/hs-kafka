module ListOffsetsRequest
  ( listOffsetsRequest
  ) where

import Data.Foldable (foldl')
import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Common
import KafkaWriter

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
     Topic
  -> [Partition]
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
      , build16 (fromIntegral clientIdLength)
      , buildArray (fromByteString clientId) clientIdLength
      -- listoffsets request
      , build32 defaultReplicaId
      , build8 defaultIsolationLevel
      , build32 1 -- number of following topics

      , build16 (size16 topicName)
      , buildArray topicName topicNameSize
      , build32 (fromIntegral partitionCount)
      , foldl'
          (\b p -> b <> foldBuilder
              [ build32 (partitionIndex p)
              , build32 defaultCurrentLeaderEpoch
              , build64 (kafkaTimestamp defaultTimestamp)
              ]
          ) mempty partitions
      ]
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 1 mempty
      writeUnliftedArray arr 0 req
      pure arr
  where
    Topic topicName _ _ = topic
    topicNameSize = sizeofByteArray topicName
    partitionCount = length partitions

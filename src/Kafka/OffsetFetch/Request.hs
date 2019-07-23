module Kafka.OffsetFetch.Request where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Writer

offsetFetchApiKey :: Int16
offsetFetchApiKey = 9

offsetFetchApiVersion :: Int16
offsetFetchApiVersion = 5

offsetFetchRequest ::
     GroupMember
  -> TopicName
  -> [Int32]
  -> UnliftedArray ByteArray
offsetFetchRequest (GroupMember gid _) (TopicName topicName) offs =
  let
    reqSize = evaluate $ foldBuilder [build32 (size32 req)]
    req =
      evaluate $ foldBuilder
        [ build16 offsetFetchApiKey
        , build16 offsetFetchApiVersion
        , build32 correlationId
        , buildString (fromByteString clientId) clientIdLength
        , buildString gid (sizeofByteArray gid)
        , build32 1 -- 1 topic
        , buildString topicName (sizeofByteArray topicName)
        , build32 (fromIntegral (length offs))
        , foldMap build32 offs
        ]
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

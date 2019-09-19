module Kafka.Internal.OffsetFetch.Request where

import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

offsetFetchApiKey :: Int16
offsetFetchApiKey = 9

offsetFetchApiVersion :: Int16
offsetFetchApiVersion = 5

offsetFetchRequest ::
     GroupMember
  -> TopicName
  -> [Int32]
  -> UnliftedArray ByteArray
offsetFetchRequest (GroupMember gid _) tn offs =
  let
    reqSize = build (int32 (size32 req))
    req =
      build $
        int16 offsetFetchApiKey
        <> int16 offsetFetchApiVersion
        <> int32 correlationId
        <> string clientId clientIdLength
        <> bytearray gid (sizeofByteArray gid)
        <> int32 1 -- 1 topic
        <> topicName tn
        <> int32 (fromIntegral (length offs))
        <> foldMap int32 offs
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

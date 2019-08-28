{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Kafka.Internal.JoinGroup.Request
  ( joinGroupRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

joinGroupApiVersion :: Int16
joinGroupApiVersion = 4

joinGroupApiKey :: Int16
joinGroupApiKey = 11

defaultSessionTimeout :: Int32
defaultSessionTimeout = 30000

defaultRebalanceTimeout :: Int32
defaultRebalanceTimeout = 30000

defaultProtocolType :: ByteArray
defaultProtocolType = fromByteString "consumer"

defaultProtocolTypeLength :: Int
defaultProtocolTypeLength = sizeofByteArray defaultProtocolType

defaultProtocolData :: ByteArray -> forall s. KafkaWriterBuilder s
defaultProtocolData topic =
  build32 1 -- 1 protocol
  <> buildString (fromByteString "range") 5 -- protocol name
  <> build32 (12 + fromIntegral topicSize) -- metadata bytes length
  <> build16 0 -- version
  <> build32 1 -- number of subscriptions
  <> buildString topic topicSize -- topic name
  <> build32 0 -- userdata bytes length
  where
    topicSize = sizeofByteArray topic

joinGroupRequest ::
     TopicName
  -> GroupMember
  -> UnliftedArray ByteArray
joinGroupRequest (TopicName topicName) (GroupMember gid mid) =
  let
    groupIdLength = sizeofByteArray gid
    reqSize = evaluate $ build32 (fromIntegral $ sizeofByteArray req)
    req = evaluate $
      build16 joinGroupApiKey
      <> build16 joinGroupApiVersion
      <> build32 correlationId
      <> buildString (fromByteString clientId) (fromIntegral clientIdLength)
      <> buildString gid (fromIntegral groupIdLength)
      <> build32 defaultSessionTimeout
      <> build32 defaultRebalanceTimeout
      <> maybe
          (build16 0)
          (\m -> buildString m (sizeofByteArray m))
          mid
      <> buildString defaultProtocolType (fromIntegral defaultProtocolTypeLength)
      <> defaultProtocolData topicName
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

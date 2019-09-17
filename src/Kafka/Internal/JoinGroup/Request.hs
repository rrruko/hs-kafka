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

defaultProtocolData :: ByteArray -> Builder
defaultProtocolData topic =
  int32 1 -- 1 protocol
  <> string (fromByteString "range") 5 -- protocol name
  <> int32 (12 + fromIntegral topicSize) -- metadata bytes length
  <> int16 0 -- version
  <> int32 1 -- number of subscriptions
  <> string topic topicSize -- topic name
  <> int32 0 -- userdata bytes length
  where
    topicSize = sizeofByteArray topic

joinGroupRequest ::
     TopicName
  -> GroupMember
  -> UnliftedArray ByteArray
joinGroupRequest (TopicName topicName) (GroupMember gid mid) =
  let
    groupIdLength = sizeofByteArray gid
    reqSize = build $ int32 (fromIntegral $ sizeofByteArray req)
    req = build $
      int16 joinGroupApiKey
      <> int16 joinGroupApiVersion
      <> int32 correlationId
      <> string (fromByteString clientId) (fromIntegral clientIdLength)
      <> string gid (fromIntegral groupIdLength)
      <> int32 defaultSessionTimeout
      <> int32 defaultRebalanceTimeout
      <> maybe
          (int16 0)
          (\m -> string m (sizeofByteArray m))
          mid
      <> string defaultProtocolType (fromIntegral defaultProtocolTypeLength)
      <> defaultProtocolData topicName
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

{-# language
    OverloadedStrings
  , RankNTypes
  , ViewPatterns
  #-}

module Kafka.Internal.JoinGroup.Request
  ( joinGroupRequest
  ) where

import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

import qualified String.Ascii as S

joinGroupApiVersion :: Int16
joinGroupApiVersion = 4

joinGroupApiKey :: Int16
joinGroupApiKey = 11

defaultSessionTimeout :: Int32
defaultSessionTimeout = 30000

defaultRebalanceTimeout :: Int32
defaultRebalanceTimeout = 30000

defaultProtocolType :: ByteArray
defaultProtocolType = S.toByteArray "consumer"

defaultProtocolTypeLength :: Int
defaultProtocolTypeLength = sizeofByteArray defaultProtocolType

defaultProtocolData :: TopicName -> Builder
defaultProtocolData topic =
  int32 1 -- 1 protocol
  <> string "range" 5 -- protocol name
  <> int32 (12 + fromIntegral topicSize) -- metadata bytes length
  <> int16 0 -- version
  <> int32 1 -- number of subscriptions
  <> topicName topic -- topic name
  <> int32 0 -- userdata bytes length
  where
    topicSize = S.length (coerce topic)

joinGroupRequest ::
     TopicName
  -> GroupMember
  -> UnliftedArray ByteArray
joinGroupRequest topic (GroupMember (GroupName gid) mid) =
  let
    groupIdLength = S.length gid
    reqSize = build $ int32 (fromIntegral $ sizeofByteArray req)
    req = build $
      int16 joinGroupApiKey
      <> int16 joinGroupApiVersion
      <> int32 correlationId
      <> string clientId (fromIntegral clientIdLength)
      <> string gid (fromIntegral groupIdLength)
      <> int32 defaultSessionTimeout
      <> int32 defaultRebalanceTimeout
      <> maybe
          (int16 0)
          (\m -> bytearray m (sizeofByteArray m))
          mid
      <> bytearray defaultProtocolType (fromIntegral defaultProtocolTypeLength)
      <> defaultProtocolData topic
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

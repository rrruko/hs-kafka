{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Kafka.JoinGroup.Request
  ( joinGroupRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Writer

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
defaultProtocolData topic = mconcat
  [ build32 1 -- 1 protocol
  , buildString (fromByteString "range") 5 -- protocol name
  , build32 16 -- metadata bytes length
  , build16 0 -- version
  , build32 1 -- number of subscriptions
  , buildString topic (sizeofByteArray topic) -- topic name
  , build32 0 -- userdata bytes length
  ]

data GroupMember = GroupMember
  { groupId :: ByteArray
  , memberId :: Maybe ByteArray
  } deriving (Eq, Show)

joinGroupRequest ::
     Topic
  -> GroupMember
  -> UnliftedArray ByteArray
joinGroupRequest (Topic topicName _ _) (GroupMember gid mid) =
  let
    groupIdLength = sizeofByteArray gid
    reqSize = fromIntegral $
        28
      + clientIdLength
      + groupIdLength
      + maybe 0 sizeofByteArray mid
    req = evaluate $ foldBuilder $
      [ build32 reqSize
      , build16 joinGroupApiKey
      , build16 joinGroupApiVersion
      , build32 correlationId
      , buildString (fromByteString clientId) (fromIntegral clientIdLength)
      , buildString gid (fromIntegral groupIdLength)
      , build32 defaultSessionTimeout
      , build32 defaultRebalanceTimeout
      , maybe
          (build16 (-1))
          (\m -> buildString m (sizeofByteArray m))
          mid
      , buildString defaultProtocolType (fromIntegral defaultProtocolTypeLength)
      , defaultProtocolData topicName
      ]
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 1 mempty
      writeUnliftedArray arr 0 req
      pure arr

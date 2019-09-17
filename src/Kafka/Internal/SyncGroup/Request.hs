{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Kafka.Internal.SyncGroup.Request
  ( syncGroupRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

syncGroupApiVersion :: Int16
syncGroupApiVersion = 2

syncGroupApiKey :: Int16
syncGroupApiKey = 14

defaultAssignmentData :: MemberAssignment -> Builder
defaultAssignmentData assignment =
  let
    assn = mconcat
      [ int16 0 -- version
      , mapArray (assignedTopics assignment)
          (\top -> mconcat
            [ string
                (assignedTopicName top)
                (sizeofByteArray $ assignedTopicName top)
            , mapArray
                (assignedPartitions top)
                (\part -> int32 part)
            ])
      , int32 0 -- userdata bytes length
      ]
  in mconcat
    [ string memId memIdSize
    , int32 (size32 (build assn))
    , assn
    ]
  where
    memId = assignedMemberId assignment
    memIdSize = fromIntegral $ sizeofByteArray memId

syncGroupRequest ::
     GroupMember
  -> GenerationId
  -> [MemberAssignment]
  -> UnliftedArray ByteArray
syncGroupRequest (GroupMember gid mid) (GenerationId genId) assignments =
  let
    groupIdLength = sizeofByteArray gid
    reqSize = build $
      int32 (fromIntegral $ sizeofByteArray req)
    req = build $
      int16 syncGroupApiKey
      <> int16 syncGroupApiVersion
      <> int32 correlationId
      <> string (fromByteString clientId) (fromIntegral clientIdLength)
      <> string gid (fromIntegral groupIdLength)
      <> int32 genId
      <> maybe
          (int16 0)
          (\m -> string m (sizeofByteArray m))
          mid
      <> mapArray assignments defaultAssignmentData
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

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

defaultAssignmentData :: MemberAssignment -> forall s. KafkaWriterBuilder s
defaultAssignmentData assignment =
  let
    assn = mconcat
      [ build16 0 -- version
      , buildMapArray (assignedTopics assignment)
          (\top -> mconcat
            [ buildString
                (assignedTopicName top)
                (sizeofByteArray $ assignedTopicName top)
            , buildMapArray
                (assignedPartitions top)
                (\part -> build32 part)
            ])
      , build32 0 -- userdata bytes length
      ]
  in mconcat
    [ buildString memId memIdSize
    , build32 (size32 $ evaluate assn)
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
    reqSize = evaluate $
      build32 (fromIntegral $ sizeofByteArray req)
    req = evaluate $
      build16 syncGroupApiKey
      <> build16 syncGroupApiVersion
      <> build32 correlationId
      <> buildString (fromByteString clientId) (fromIntegral clientIdLength)
      <> buildString gid (fromIntegral groupIdLength)
      <> build32 genId
      <> maybe
          (build16 0)
          (\m -> buildString m (sizeofByteArray m))
          mid
      <> buildMapArray assignments defaultAssignmentData
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

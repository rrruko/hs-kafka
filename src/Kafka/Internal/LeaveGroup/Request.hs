module Kafka.Internal.LeaveGroup.Request
  ( leaveGroupRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

leaveGroupApiVersion :: Int16
leaveGroupApiVersion = 2

leaveGroupApiKey :: Int16
leaveGroupApiKey = 13

leaveGroupRequest ::
     GroupMember
  -> UnliftedArray ByteArray
leaveGroupRequest (GroupMember gid mid) =
  let
    groupIdLength = sizeofByteArray gid
    reqSize = evaluate $ (build32 (fromIntegral $ sizeofByteArray req))
    req = evaluate $
      build16 leaveGroupApiKey
      <> build16 leaveGroupApiVersion
      <> build32 correlationId
      <> buildString (fromByteString clientId) (fromIntegral clientIdLength)
      <> buildString gid (fromIntegral groupIdLength)
      <> maybe
          (build16 0)
          (\m -> buildString m (sizeofByteArray m))
          mid
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

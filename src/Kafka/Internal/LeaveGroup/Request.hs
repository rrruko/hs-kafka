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
    reqSize = build $ (int32 (fromIntegral $ sizeofByteArray req))
    req = build $
      int16 leaveGroupApiKey
      <> int16 leaveGroupApiVersion
      <> int32 correlationId
      <> string (fromByteString clientId) (fromIntegral clientIdLength)
      <> string gid (fromIntegral groupIdLength)
      <> maybe
          (int16 0)
          (\m -> string m (sizeofByteArray m))
          mid
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

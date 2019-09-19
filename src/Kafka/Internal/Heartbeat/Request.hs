module Kafka.Internal.Heartbeat.Request
  ( heartbeatRequest
  ) where

import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

heartbeatApiVersion :: Int16
heartbeatApiVersion = 2

heartbeatApiKey :: Int16
heartbeatApiKey = 12

heartbeatRequest ::
     GroupMember
  -> GenerationId
  -> UnliftedArray ByteArray
heartbeatRequest (GroupMember gid mid) (GenerationId genId) =
  let
    groupIdLength = sizeofByteArray gid
    reqSize = build $ int32 (fromIntegral $ sizeofByteArray req)
    req = build $
      int16 heartbeatApiKey
      <> int16 heartbeatApiVersion
      <> int32 correlationId
      <> string clientId (fromIntegral clientIdLength)
      <> bytearray gid (fromIntegral groupIdLength)
      <> int32 genId
      <> maybe
          (int16 0)
          (\m -> bytearray m (sizeofByteArray m))
          mid
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

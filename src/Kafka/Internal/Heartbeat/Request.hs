module Kafka.Internal.Heartbeat.Request
  ( heartbeatRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
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
    reqSize = evaluate $ build32 (fromIntegral $ sizeofByteArray req)
    req = evaluate $
      build16 heartbeatApiKey
      <> build16 heartbeatApiVersion
      <> build32 correlationId
      <> buildString (fromByteString clientId) (fromIntegral clientIdLength)
      <> buildString gid (fromIntegral groupIdLength)
      <> build32 genId
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

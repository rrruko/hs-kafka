module HeartbeatRequest
  ( heartbeat
  ) where

import Data.Int
import Data.Primitive.ByteArray

import Common
import KafkaWriter

heartbeatApiKey :: Int16
heartbeatApiKey = 12

heartbeatApiVersion :: Int16
heartbeatApiVersion = 2

heartbeat :: ByteArray -> Int32 -> ByteArray -> ByteArray
heartbeat groupId generationId memberId = 
  let
    totalSize =
        16
      + clientIdLength
      + sizeofByteArray groupId
      + sizeofByteArray memberId
  in
    evaluateWriter totalSize $ do
      -- size
      writeBE32 (fromIntegral $ totalSize - 4)
      -- request header
      writeBE16 heartbeatApiKey
      writeBE16 heartbeatApiVersion
      writeBE32 correlationId
      writeArray (fromByteString clientId) clientIdLength
  
      -- actual heartbeat request
      writeArray groupId (sizeofByteArray groupId)
      writeBE32 generationId
      writeArray memberId (sizeofByteArray memberId)

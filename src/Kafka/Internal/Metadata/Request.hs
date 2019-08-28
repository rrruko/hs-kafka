module Kafka.Internal.Metadata.Request
  ( metadataRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

metadataApiVersion :: Int16
metadataApiVersion = 7

metadataApiKey :: Int16
metadataApiKey = 3

metadataRequest ::
     TopicName
  -> AutoCreateTopic
  -> UnliftedArray ByteArray
metadataRequest (TopicName topicName) autoCreate =
  let
    reqSize = evaluate $ (build32 (fromIntegral $ sizeofByteArray req))
    req = evaluate $
      build16 metadataApiKey
      <> build16 metadataApiVersion
      <> build32 correlationId
      <> buildString (fromByteString clientId) (fromIntegral clientIdLength)
      <> build32 1
      <> buildString topicName (sizeofByteArray topicName)
      <> buildBool (case autoCreate of Create -> True; NeverCreate -> False)
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

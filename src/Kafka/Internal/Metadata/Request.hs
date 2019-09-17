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
    reqSize = build $ (int32 (fromIntegral $ sizeofByteArray req))
    req = build $
      int16 metadataApiKey
      <> int16 metadataApiVersion
      <> int32 correlationId
      <> string (fromByteString clientId) (fromIntegral clientIdLength)
      <> int32 1
      <> string topicName (sizeofByteArray topicName)
      <> bool (case autoCreate of Create -> True; NeverCreate -> False)
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

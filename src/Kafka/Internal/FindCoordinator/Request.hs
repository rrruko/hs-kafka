{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Kafka.Internal.FindCoordinator.Request
  ( findCoordinatorRequest
  ) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array

import Kafka.Common
import Kafka.Internal.Writer

findCoordinatorApiVersion :: Int16
findCoordinatorApiVersion = 2

findCoordinatorApiKey :: Int16
findCoordinatorApiKey = 10

findCoordinatorRequest ::
     ByteArray -- key, a.k.a. group name
  -> Int8
  -> UnliftedArray ByteArray
findCoordinatorRequest !key !keyType =
  let
    keyLength = sizeofByteArray key
    reqSize = evaluate $
      build32 (fromIntegral $ sizeofByteArray req)
    req = evaluate $
      build16 findCoordinatorApiKey
      <> build16 findCoordinatorApiVersion
      <> build32 correlationId
      <> buildString (fromByteString clientId) (fromIntegral clientIdLength)
      <> buildString key (fromIntegral keyLength)
      <> build8 keyType
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr

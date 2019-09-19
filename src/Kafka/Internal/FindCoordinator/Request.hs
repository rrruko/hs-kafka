{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module Kafka.Internal.FindCoordinator.Request
  ( findCoordinatorRequest
  ) where

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
    reqSize = build $
      int32 (fromIntegral $ sizeofByteArray req)
    req = build $
      int16 findCoordinatorApiKey
      <> int16 findCoordinatorApiVersion
      <> int32 correlationId
      <> string clientId (fromIntegral clientIdLength)
      <> bytearray key (fromIntegral keyLength)
      <> int8 keyType
  in
    runUnliftedArray $ do
      arr <- newUnliftedArray 2 mempty
      writeUnliftedArray arr 0 reqSize
      writeUnliftedArray arr 1 req
      pure arr


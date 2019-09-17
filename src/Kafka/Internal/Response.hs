{-# LANGUAGE LambdaCase #-}

module Kafka.Internal.Response
  ( fromKafkaResponse
  , getKafkaResponse
  , getResponseSizeHeader
  , tryParse
  ) where

import Data.Attoparsec.ByteString (Parser, parseOnly)
import Data.Bifunctor
import Data.ByteString (ByteString)
import Data.Bytes.Types
import Data.Primitive.ByteArray
import Data.Word
import GHC.Conc
import Socket.Stream.Interruptible.MutableBytes
import System.IO

import Kafka.Common

getKafkaResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException ByteString)
getKafkaResponse kafka interrupt = do
  getResponseSizeHeader kafka interrupt >>= \case
    Right responseByteCount -> do
      responseBuffer <- newByteArray responseByteCount
      let responseBufferSlice = MutableBytes responseBuffer 0 responseByteCount
      responseStatus <- first KafkaReceiveException <$>
        receiveExactly
          interrupt
          (getKafka kafka)
          responseBufferSlice
      responseBytes <- toByteString <$> unsafeFreezeByteArray responseBuffer
      pure $ responseBytes <$ responseStatus
    Left e -> pure $ Left e

getResponseSizeHeader ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException Int)
getResponseSizeHeader kafka interrupt = do
  responseSizeBuf <- newByteArray 4
  responseStatus <- first KafkaReceiveException <$>
    receiveExactly
      interrupt
      (getKafka kafka)
      (MutableBytes responseSizeBuf 0 4)
  byteCount <- fromIntegral . byteSwap32 <$> readByteArray responseSizeBuf 0
  pure $ byteCount <$ responseStatus

logMaybe :: Show a => a -> Maybe Handle -> IO ()
logMaybe a = \case
  Nothing -> pure ()
  Just h -> do
    hPutStr h (show a ++ "\n\n")
    hFlush h

fromKafkaResponse :: (Show a)
  => Parser a
  -> Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String a))
fromKafkaResponse parser kafka interrupt debugHandle =
  getKafkaResponse kafka interrupt >>= \case
    Right bytes -> do
      let res = parseOnly parser bytes
      logMaybe res debugHandle
      pure (Right res)
    Left err -> pure (Left err)

tryParse :: Either KafkaException (Either String a) -> Either KafkaException a
tryParse = \case
  Right (Right parsed) -> Right parsed
  Right (Left parseError) -> Left (KafkaParseException parseError)
  Left networkError -> Left networkError

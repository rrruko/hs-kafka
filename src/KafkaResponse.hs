{-# LANGUAGE LambdaCase #-}

module KafkaResponse
  ( fromKafkaResponse
  , getKafkaResponse
  , getResponseSizeHeader
  ) where

import Common
import Data.Attoparsec.ByteString (Parser, parseOnly)
import Data.Bifunctor
import Data.ByteString
import Data.Bytes.Types
import Data.Primitive.ByteArray
import Data.Word
import GHC.Conc
import Socket.Stream.Interruptible.MutableBytes

getKafkaResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException ByteString)
getKafkaResponse kafka interrupt = do
  getResponseSizeHeader kafka interrupt >>= \case
    Right responseByteCount -> do
      responseBuffer <- newByteArray responseByteCount
      let responseBufferSlice = MutableBytes responseBuffer 0 responseByteCount
      responseStatus <- first toKafkaException <$>
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
  responseStatus <- first toKafkaException <$>
    receiveExactly
      interrupt
      (getKafka kafka)
      (MutableBytes responseSizeBuf 0 4)
  byteCount <- fromIntegral . byteSwap32 <$> readByteArray responseSizeBuf 0
  pure $ byteCount <$ responseStatus

fromKafkaResponse ::
     Parser a
  -> Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String a))
fromKafkaResponse parser kafka interrupt =
  (fmap . fmap)
    (parseOnly parser)
    (getKafkaResponse kafka interrupt)

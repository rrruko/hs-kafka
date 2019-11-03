{-# language
    BangPatterns
  , LambdaCase
  , RankNTypes
  , ScopedTypeVariables
  #-}

module Kafka.Internal.Combinator
  ( Parser
  , array
  , count
  , bool
  , bytearray
  , topicName
  , int8
  , int16
  , int32
  , int64
  , nullableArray
  , sizedBytes
  , nullableByteArray
  , nullableByteArrayVar
  , nullableBytes
  , nullableSequence
  , takeByteArray
  , varInt
  , (Smith.<?>)
  )
  where

import Control.Monad (replicateM)
import Kafka.Common
--import Data.Bytes.Parser ((<?>))

import qualified Data.Bytes as B
import qualified Data.Bytes.Parser as Smith
import qualified Data.Bytes.Parser.BigEndian as Smith
--import qualified Data.Primitive.Contiguous as C
import qualified String.Ascii as S

type Parser a = forall s. Smith.Parser String s a

int8 :: String -> Parser Int8
int8 = Smith.int8

int16 :: String -> Parser Int16
int16 = Smith.int16

int32 :: String -> Parser Int32
int32 = Smith.int32

int64 :: String -> Parser Int64
int64 = Smith.int64

bool :: String -> Parser Bool
bool e = Smith.word8 e >>= \case
  0 -> pure False
  _ -> pure True

count :: Integral i => i -> Parser a -> Parser [a]
count = replicateM . fromIntegral
{-# inlineable count #-}

{-
c_array :: forall arr a. (Contiguous arr, Element arr a)
  => Parser a
  -> Parser (arr a)
c_array p = do
  len <- fromIntegral <$> int32 "c_array: len"
  marr :: Mutable arr s a <- Smith.effect (C.new len)
  let go :: Int -> Smith.Parser String s ()
      go !ix = if ix < len
        then do
          (a :: a) <- p <?> ("c_array: element " <> show ix)
          Smith.effect (C.write marr ix a)
          go (ix + 1)
        else do
          pure ()
  go 0
  Smith.effect (C.unsafeFreeze marr)
{-# inlineable c_array #-}
-}

--nullableArray :: forall arr a. (Contiguous arr, Element arr a)
array :: Parser a -> Parser [a]
array p = do
  arraySize <- int32 "array: arraySize"
  count arraySize p

nullableArray :: Parser a -> Parser [a]
nullableArray p = do
  arraySize <- int32 "nullableArray: array size"
  if arraySize <= 0
    then pure []
    else count arraySize p

bytearray :: Parser ByteArray
bytearray = do
  len <- int16 "bytearray: len"
  bytes <- Smith.take ("take " <> show len) (fromIntegral len)
  pure (B.toByteArray bytes)

topicName :: Parser TopicName
topicName = do
  b <- bytearray
  case S.fromByteArray b of
    Nothing -> Smith.fail "topicName: non-ascii"
    Just str -> pure (TopicName str)

sizedBytes :: Parser ByteArray
sizedBytes = do
  len <- int32 "sizedBytes: len"
  bytes <- Smith.take ("take " <> show len) (fromIntegral len)
  pure (B.toByteArray bytes)

nullableByteArray :: Parser (Maybe ByteArray)
nullableByteArray = do
  len <- int16 "nullableByteArray: len"
  if len < 0
    then pure Nothing
    else do
      bytes <- Smith.take ("take " <> show len) (fromIntegral len)
      pure (Just (B.toByteArray bytes))

nullableByteArrayVar :: Parser (Maybe ByteArray)
nullableByteArrayVar = do
  len <- varInt
  if len < 0
    then pure Nothing
    else do
      bytes <- Smith.take ("take " <> show len) (fromIntegral len)
      pure (Just (B.toByteArray bytes))

nullableBytes :: Parser a -> Parser (Maybe a)
nullableBytes p = do
  len <- int32 "nullableBytes: len"
  if len <= 0
    then pure Nothing
    else Just <$> p

takeByteArray :: Parser ByteArray
takeByteArray = do
  bytes <- Smith.remaining
  pure (B.toByteArray bytes)

many :: Parser a -> Parser [a]
many v = many_v
  where
     many_v = some_v `Smith.orElse` pure []
     some_v = liftA2 (:) v many_v

nullableSequence :: Parser a -> Parser (Maybe [a])
nullableSequence p = do
  len <- int32 "nullableSequence: len"
  bytes <- Smith.take ("take " <> show len) (fromIntegral len)
  if len <= 0
    then pure Nothing
    else case Smith.parseBytes (many p) bytes of
      Smith.Failure _ -> pure Nothing
      Smith.Success (Smith.Slice _ _ as) -> pure (Just as)

varInt :: Parser Int
varInt = fmap unZigZag (go 1)
  where
    go :: Int -> Parser Int
    go !n = do
      b <- fromIntegral <$> Smith.any ("varInt: " <> show n)
      if testBit b 7
        then do
          rest <- go (n * 128)
          pure (clearBit b 7 * n + rest)
        else do
          pure (b * n)

unZigZag :: Int -> Int
unZigZag n
  | even n = n `div` 2
  | otherwise = (-1) * ((n + 1) `div` 2)

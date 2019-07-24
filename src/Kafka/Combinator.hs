module Kafka.Combinator 
  ( array
  , byteString
  , count
  , int8
  , int16
  , int32
  , int64
  , networkByteOrder
  , nullableArray
  , nullableByteString
  , nullableBytes
  , nullableSequence
  , parseVarint
  , sizedBytes
  ) where

import Control.Applicative (many)
import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.Bits
import Data.ByteString (ByteString)
import Data.Int

import qualified Data.Attoparsec.ByteString as AT

int8 :: Parser Int8
int8 = fromIntegral <$> AT.anyWord8

int16 :: Parser Int16
int16 = networkByteOrder . map fromIntegral <$> AT.count 2 AT.anyWord8

int32 :: Parser Int32
int32 = networkByteOrder . map fromIntegral <$> AT.count 4 AT.anyWord8

int64 :: Parser Int64
int64 = networkByteOrder . map fromIntegral <$> AT.count 8 AT.anyWord8

networkByteOrder :: Integral a => [Word] -> a
networkByteOrder = 
  fst . foldr 
    (\byte (acc, i) -> (acc + fromIntegral byte * i, i * 0x100))
    (0, 1)

count :: Integral n => n -> Parser a -> Parser [a]
count = AT.count . fromIntegral

array :: Parser a -> Parser [a]
array p = do
  arraySize <- int32
  count arraySize p

nullableArray :: Parser a -> Parser [a]
nullableArray p = do
  arraySize <- int32
  if arraySize <= 0
    then pure []
    else count arraySize p

byteString :: Parser ByteString
byteString = do
  stringLength <- int16 <?> "string length"
  AT.take (fromIntegral stringLength) <?> "string contents"

sizedBytes :: Parser ByteString
sizedBytes = do
  bytesLength <- int32 <?> "bytes length"
  AT.take (fromIntegral bytesLength) <?> "bytes"

parseVarint :: Parser Int
parseVarint = unZigzag <$> go 1
  where
    go :: Int -> Parser Int
    go n = do
      b <- fromIntegral <$> AT.anyWord8
      case testBit b 7 of
        True -> do
          rest <- go (n * 128)
          pure (clearBit b 7 * n + rest)
        False -> pure (b * n)

unZigzag :: Int -> Int
unZigzag n
  | even n    = n `div` 2
  | otherwise = (-1) * ((n + 1) `div` 2)

nullableByteString :: Int -> Parser (Maybe ByteString)
nullableByteString n
  | n < 0 = pure Nothing
  | otherwise = Just <$> AT.take n

nullableBytes :: Parser a -> Parser (Maybe a)
nullableBytes p = do
  bytesLength <- int32
  if bytesLength <= 0 then
    pure Nothing
  else
    Just <$> p

nullableSequence :: Parser a -> Parser (Maybe [a])
nullableSequence p = do
  bytesLength <- int32
  bytes <- AT.take (fromIntegral bytesLength)
  if bytesLength <= 0 then
    pure Nothing
  else
    case AT.parseOnly (many p) bytes of
      Right r -> pure (Just r)
      Left e -> fail e

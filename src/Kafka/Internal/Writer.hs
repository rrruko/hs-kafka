module Kafka.Internal.Writer
  ( module B
  , mapArray
  , string
  , array
  , size32
  , bool

  , int8
  , int16
  , int32
  , int64
  ) where

import Data.Int (Int8,Int16,Int32,Int64)
import Data.Primitive.ByteArray
import Data.Word (byteSwap16,byteSwap32,byteSwap64)

import qualified Builder as B
import Builder as B hiding (int8,int16,int32,int64)

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

mapArray :: Foldable t => t a -> (a -> B.Builder) -> B.Builder
mapArray xs f = int32 (fromIntegral (length xs)) <> foldMap f xs

string :: ByteArray -> Int -> B.Builder
string src len = int16 (fromIntegral len) <> B.bytearray src 0 len

array :: [B.Builder] -> Int -> B.Builder
array src len = int32 (fromIntegral len) <> mconcat src

bool :: Bool -> B.Builder
bool b = int8 (if b then 1 else 0)

int8 :: Int8 -> B.Builder
int8 = B.int8

int16 :: Int16 -> B.Builder
int16 = B.int16 . fromIntegral . byteSwap16 . fromIntegral

int32 :: Int32 -> B.Builder
int32 = B.int32 . fromIntegral . byteSwap32 . fromIntegral

int64 :: Int64 -> B.Builder
int64 = B.int64 . fromIntegral . byteSwap64 . fromIntegral



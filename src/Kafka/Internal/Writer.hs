module Kafka.Internal.Writer
  ( module B
  , mapArray
  , topicName
  , string
  , bytearray
  , array
  , size32
  , bool

  , int8
  , int16
  , int32
  , int64
  ) where

import Data.Word (byteSwap16,byteSwap32,byteSwap64)

import Kafka.Common

import qualified Builder as B
import Builder as B hiding (int8,int16,int32,int64,bytearray)
import qualified String.Ascii as S

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

mapArray :: Foldable t => t a -> (a -> B.Builder) -> B.Builder
mapArray xs f = int32 (fromIntegral (length xs)) <> foldMap f xs

bytearray :: ByteArray -> Int -> B.Builder
bytearray src len = int16 (fromIntegral len)
  <> B.bytearray src 0 len

string :: S.String -> Int -> B.Builder
string src len = int16 (fromIntegral len)
  <> (S.asByteArray src $ \b -> B.bytearray b 0 len)

topicName :: TopicName -> B.Builder
topicName (TopicName tn) = string tn (S.length tn)

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



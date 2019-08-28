module Kafka.Internal.Zigzag
  ( zigzag
  ) where

import Data.Bifunctor (bimap)
import Data.Bits ((.|.))
import Data.List.NonEmpty
import Data.Primitive.ByteArray (ByteArray, byteArrayFromList)
import Data.Tuple (swap)
import Data.Word (Word8)

varint :: Int -> ByteArray
varint n =
  let
    chunks :: NonEmpty Word8
    chunks = setMsb $ chunk n
    setMsb :: NonEmpty Word8 -> NonEmpty Word8
    setMsb (x :| xs) =
      let rest = setMsb <$> nonEmpty xs
      in  case rest of
            Just r -> (128 .|. x) <| r
            Nothing -> pure x
  in
    byteArrayFromList (toList chunks)

zigzag :: Int -> ByteArray
zigzag n
  | n >= 0 = varint (n * 2)
  | otherwise = varint ((-n) * 2 - 1)

chunk :: Int -> NonEmpty Word8
chunk = unfoldr $ \n -> if n < 128
  then (fromIntegral n, Nothing)
  else swap
    . bimap Just intToWord8
    $ divMod n 128

intToWord8 :: Int -> Word8
intToWord8 = fromIntegral

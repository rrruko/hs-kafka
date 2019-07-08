module Varint (varint, zigzag) where

import Data.Bits ((.|.))
import Data.List (unfoldr)
import Data.Primitive.ByteArray (ByteArray, byteArrayFromList)
import Data.Tuple (swap)
import Data.Word (Word8)

varint :: Int -> ByteArray
varint n =
  let
    chunks = setMsb $ chunk n
    setMsb [x] = [x]
    setMsb (x:xs) = (128 .|. x) : setMsb xs
  in
    byteArrayFromList chunks

zigzag :: Int -> ByteArray
zigzag n
  | n >= 0 = varint (n * 2)
  | n < 0 = varint ((-n) * 2 - 1)

chunk :: Int -> [Word8]
chunk 0 = [0]
chunk m =
  unfoldr
    (\n ->
      if n == 0
        then Nothing
        else Just (swap . fmap fromIntegral $ divMod n 128))
    m

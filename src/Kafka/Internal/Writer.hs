{-# language
    BangPatterns
  , MagicHash
  , RankNTypes
  , UnboxedTuples
  #-}

module Kafka.Internal.Writer
  (
    -- * Builder type
    KafkaWriterBuilder
    -- * Evaluating a builder
  , evaluate
    -- * Constructing a builder
  , build8
  , build16
  , build32
  , build64
  , buildArray
  , buildBool
  , buildMapArray
  , buildString

    -- * Helper function
  , size32
  ) where

import Data.Int
import Data.Primitive (Prim(..))
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned
import Data.Word (byteSwap16, byteSwap32, byteSwap64)
import GHC.Exts

import qualified Data.List as L

newtype KafkaWriter s = K
  (    MutableByteArray# s -- ^ buffer
    -> Int# -- ^ offset into the buffer
    -> (State# s -> (# State# s, Int# #)) -- ^ update to offset
  )

instance Semigroup (KafkaWriter s) where
  K x <> K y = K $ \marr# ix0# s0# -> case x marr# ix0# s0# of
    (# s1#, ix1# #) -> y marr# ix1# s1#
  {-# inline (<>) #-}

instance Monoid (KafkaWriter s) where
  mempty = K $ \_ ix0# s0# -> (# s0#, ix0# #)
  {-# inline mempty #-}

writeUnaligned :: (Prim a, PrimUnaligned a)
  => a
  -> KafkaWriter s
writeUnaligned a = K $ \marr# ix0# s0# ->
  case writeUnalignedByteArray# marr# ix0# a s0# of
    s1# -> (# s1#, ix0# +# alignment# a #)
{-# inline writeUnaligned #-}

write8 :: Int8 -> KafkaWriter s
write8 = writeUnaligned

write16 :: Int16 -> KafkaWriter s
write16 = writeUnaligned . toBE16

write32 :: Int32 -> KafkaWriter s
write32 = writeUnaligned . toBE32

write64 :: Int64 -> KafkaWriter s
write64 = writeUnaligned . toBE64

build8 :: Int8 -> KafkaWriterBuilder s
build8 i = Kwb 1 (write8 i)

build16 :: Int16 -> KafkaWriterBuilder s
build16 i = Kwb 2 (write16 i)

build32 :: Int32 -> KafkaWriterBuilder s
build32 i = Kwb 4 (write32 i)

build64 :: Int64 -> KafkaWriterBuilder s
build64 i = Kwb 8 (write64 i)

buildBool :: Bool -> KafkaWriterBuilder s
buildBool b = Kwb 1 (write8 (if b then 1 else 0)) -- use dataToTag#?

writeBytes ::
     ByteArray
  -> Int
  -> KafkaWriter s
writeBytes (ByteArray src#) (I# len#) = K $ \marr# ix0# s0# ->
  case copyByteArray# src# 0# marr# ix0# len# s0# of
    s1# -> (# s1#, ix0# +# len# #)
{-# inline writeBytes #-}

buildBytes :: ByteArray -> Int -> KafkaWriterBuilder s
buildBytes src len = Kwb len (writeBytes src len)

buildArray :: [KafkaWriterBuilder s] -> Int -> KafkaWriterBuilder s
buildArray src len = build32 (fromIntegral len) <> mconcat src

buildMapArray :: (Foldable t)
  => t a
  -> (a -> KafkaWriterBuilder s)
  -> KafkaWriterBuilder s
buildMapArray xs f = build32 (fromIntegral $ length xs) <> foldMap f xs

buildString :: ByteArray -> Int -> KafkaWriterBuilder s
buildString src len = build16 (fromIntegral len) <> buildBytes src len

evaluate :: (forall s. KafkaWriterBuilder s) -> ByteArray
evaluate (Kwb (I# len#) kw) = case runRW# (runKafkaWriter# len# kw) of
  (# _, b# #) -> ByteArray b#
{-# inline evaluate #-}

runKafkaWriter# :: Int# -> KafkaWriter s -> State# s -> (# State# s, ByteArray# #)
runKafkaWriter# sz# (K g) = \s0# -> case newByteArray# sz# s0# of
  (# s1#, marr# #) -> case g marr# 0# s1# of
    (# s2#, _ #) -> case unsafeFreezeByteArray# marr# s2# of
      (# s3#, b# #) -> (# s3#, b# #)
{-# inline runKafkaWriter# #-}

data KafkaWriterBuilder s = Kwb
  !Int -- ^ length
  !(KafkaWriter s)

instance Semigroup (KafkaWriterBuilder s) where
  Kwb len1 x <> Kwb len2 y = Kwb (len1 + len2) (x <> y)
  {-# inline (<>) #-}

instance Monoid (KafkaWriterBuilder s) where
  mempty = Kwb 0 mempty
  {-# inline mempty #-}
  mconcat = mconcatBuilder
  {-# inline mconcat #-}

mconcatBuilder :: [KafkaWriterBuilder s] -> KafkaWriterBuilder s
mconcatBuilder = L.foldr (<>) mempty
{-# noinline[1] mconcatBuilder #-}

{-# RULES "builder_mconcat0"
  forall (x0 :: KafkaWriterBuilder s).
    mconcatBuilder [x0] = x0
  #-}
{-# RULES "builder_mconcat1"
  forall (x0 :: KafkaWriterBuilder s) x1.
    mconcatBuilder [x0,x1] = x0<>x1
  #-}
{-# RULES "builder_mconcat2"
  forall (x0 :: KafkaWriterBuilder s) x1 x2.
    mconcatBuilder [x0,x1,x2] = x0<>x1<>x2
  #-}
{-# RULES "builder_mconcat3"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3.
    mconcatBuilder [x0,x1,x2,x3] = x0<>x1<>x2<>x3
  #-}
{-# RULES "builder_mconcat4"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4.
    mconcatBuilder [x0,x1,x2,x3,x4] = x0<>x1<>x2<>x3<>x4
  #-}
{-# RULES "builder_mconcat5"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5.
    mconcatBuilder [x0,x1,x2,x3,x4,x5] = x0<>x1<>x2<>x3<>x4<>x5
  #-}
{-# RULES "builder_mconcat6"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6.
    mconcatBuilder [x0,x1,x2,x3,x4,x5,x6] = x0<>x1<>x2<>x3<>x4<>x5<>x6
  #-}
{-# RULES "builder_mconcat7"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7.
    mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7
  #-}
{-# RULES "builder_mconcat8"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8.
    mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8
  #-}
{-# RULES "builder_mconcat9"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9.
    mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9
  #-}
{-# RULES "builder_mconcat10"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10.
    mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10
  #-}
{-# RULES "builder_mconcat11"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11.
    mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11
  #-}
{-# RULES "builder_mconcat12"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12
  #-}
{-# RULES "builder_mconcat13"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13
  #-}
{-# RULES "builder_mconcat14"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14
  #-}
{-# RULES "builder_mconcat15"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15
  #-}
{-# RULES "builder_mconcat16"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16
  #-}
{-# RULES "builder_mconcat17"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17
  #-}
{-# RULES "builder_mconcat18"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18
  #-}
{-# RULES "builder_mconcat19"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19
  #-}
{-# RULES "builder_mconcat20"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20
  #-}

toBE16 :: Int16 -> Int16
toBE16 = fromIntegral . byteSwap16 . fromIntegral

toBE32 :: Int32 -> Int32
toBE32 = fromIntegral . byteSwap32 . fromIntegral

toBE64 :: Int64 -> Int64
toBE64 = fromIntegral . byteSwap64 . fromIntegral

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

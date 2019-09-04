{-# language
    BangPatterns
  , MagicHash
  , RankNTypes
  , UnboxedTuples
  #-}

{-# OPTIONS_GHC
   -ddump-simpl
   -dsuppress-all
   -ddump-to-file
   -fno-worker-wrapper
   -O2
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

  , foo
  ) where

import Data.Int
import Data.Primitive (Prim(..))
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned
import Data.Semigroup (Semigroup(..))
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
  (<>) = appendBuilder
  {-# inline (<>) #-}
  --stimes = stimesBuilder
  --{-# inline stimes #-}

instance Monoid (KafkaWriterBuilder s) where
  mempty = Kwb 0 mempty
  {-# inline mempty #-}
  mconcat = mconcatBuilder
  {-# inline mconcat #-}

toBE16 :: Int16 -> Int16
toBE16 = fromIntegral . byteSwap16 . fromIntegral

toBE32 :: Int32 -> Int32
toBE32 = fromIntegral . byteSwap32 . fromIntegral

toBE64 :: Int64 -> Int64
toBE64 = fromIntegral . byteSwap64 . fromIntegral

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

appendBuilder :: KafkaWriterBuilder s -> KafkaWriterBuilder s -> KafkaWriterBuilder s
appendBuilder (Kwb len1 x) (Kwb len2 y) = Kwb (len1 + len2) (x <> y)
{-# noinline[1] appendBuilder #-}

mconcatBuilder :: [KafkaWriterBuilder s] -> KafkaWriterBuilder s
mconcatBuilder = L.foldr (<>) mempty
{-# noinline[1] mconcatBuilder #-}

foo :: KafkaWriterBuilder s
foo = mconcat [xf,xf,xf]
--(replicate 6 (build32 130))

xf :: KafkaWriterBuilder s
xf = build32 130
{-# noinline[1] xf #-}

{-
stimesBuilder :: Integral i
  => i -> KafkaWriterBuilder s -> KafkaWriterBuilder s
stimesBuilder y0 x0
  | y0 <= 0 = mempty
  | otherwise = f x0 y0
  where
    f x y
      | even y = f (x <> x) (y `quot` 2)
      | y == 1 = x
      | otherwise = g (x <> x) (y `quot` 2) x
    g x y z
      | even y = g (x <> x) (y `quot` 2) z
      | y == 1 = x <> z
      | otherwise = g (x <> x) (y `quot` 2) (x <> z)
{-# noinline[0] stimesBuilder #-}

{-# RULES "mconcat/replicate"
  forall (x :: KafkaWriterBuilder s) (n :: Int).
    mconcatBuilder (take n (repeat x)) = stimesBuilder n x
  #-}

{-# RULES "stimesBuilder0" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (0 :: Int) x = mempty
  #-}
{-# RULES "stimesBuilder1" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (1 :: Int) x = x
  #-}
{-# RULES "stimesBuilder2" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (2 :: Int) x = x<>x
  #-}
{-# RULES "stimesBuilder3" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (3 :: Int) x = x<>x<>x
  #-}
{-# RULES "stimesBuilder4" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (4 :: Int) x = x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder5" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (5 :: Int) x = x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder6" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (6 :: Int) x = x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder7" [1]
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (7 :: Int) x = x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder8"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (8 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder9"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (9 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder10"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (10 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder11"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (11 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder12"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (12 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder13"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (13 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder14"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (14 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder15"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (15 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder16"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (16 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder17"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (17 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder18"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (18 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder19"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (19 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder20"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (20 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder21"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (21 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder22"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (22 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder23"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (23 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder24"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (24 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder25"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (25 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder26"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (26 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder27"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (27 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder28"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (28 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder29"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (29 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder30"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (30 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder31"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (31 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder32"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (32 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder33"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (33 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder34"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (34 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
{-# RULES "stimesBuilder35"
  forall (x :: KafkaWriterBuilder s).
    stimesBuilder (35 :: Int) x = x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x<>x
  #-}
-}

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
{-# RULES "builder_mconcat21"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21
  #-}
{-# RULES "builder_mconcat22"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22
  #-}
{-# RULES "builder_mconcat23"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23
  #-}
{-# RULES "builder_mconcat24"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24
  #-}
{-# RULES "builder_mconcat25"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25
  #-}
{-# RULES "builder_mconcat26"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25 x26.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25<>x26
  #-}
{-# RULES "builder_mconcat27"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25 x26 x27.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25<>x26<>x27
  #-}
{-# RULES "builder_mconcat28"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25 x26 x27 x28.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25<>x26<>x27<>x28
  #-}
{-# RULES "builder_mconcat29"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25 x26 x27 x28 x29.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28,x29]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25<>x26<>x27<>x28<>x29
  #-}
{-# RULES "builder_mconcat30"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25 x26 x27 x28 x29 x30.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28,x29,x30]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25<>x26<>x27<>x28<>x29<>x30
  #-}
{-# RULES "builder_mconcat31"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25 x26 x27 x28 x29 x30 x31.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28,x29,x30,x31]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25<>x26<>x27<>x28<>x29<>x30<>x31
  #-}
{-# RULES "builder_mconcat32"
  forall (x0 :: KafkaWriterBuilder s) x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19 x20 x21 x22 x23 x24 x25 x26 x27 x28 x29 x30 x31 x32.
     mconcatBuilder [x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28,x29,x30,x31,x32]
      = x0<>x1<>x2<>x3<>x4<>x5<>x6<>x7<>x8<>x9<>x10<>x11<>x12<>x13<>x14<>x15<>x16<>x17<>x18<>x19<>x20<>x21<>x22<>x23<>x24<>x25<>x26<>x27<>x28<>x29<>x30<>x31<>x32
  #-}


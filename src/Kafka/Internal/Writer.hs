{-# language
        BangPatterns
      , DataKinds
      , DeriveFunctor
      , GeneralizedNewtypeDeriving
      , MagicHash
      , MultiParamTypeClasses
      , PolyKinds
      , RankNTypes
      , ScopedTypeVariables
      , TypeFamilies
      , UnboxedTuples
      , UndecidableInstances
  #-}

module Kafka.Internal.Writer
  ( KafkaWriter(..)
  , KafkaWriterBuilder(..)
  , build8
  , build16
  , build32
  , build64
  , buildArray
  , buildBool
  , buildMapArray
  , buildString
  , evaluate
  , size32
  , withCtx
  , write8
  , write16
  , write32
  , write64
  , writeBytes
  , writeNum
  ) where

import Control.Applicative (liftA2)
import Control.Monad.Primitive
import Data.Int
import Data.Primitive (Prim(..), alignment)
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned
import Data.Word (byteSwap16, byteSwap32, byteSwap64)
import GHC.Exts

newtype KafkaWriter s a = K
  { getK :: ()
      => MutableByteArray# s
      -> Int#
      -> State# s
      -> (# State# s, Int#, a #)
  }

instance Functor (KafkaWriter s) where
  fmap f (K g) = K $ \marr# ix0# s0# -> case g marr# ix0# s0# of
    (# s1#, ix1#, a #) -> (# s1#, ix1#, f a #)
  {-# inline fmap #-}

instance Applicative (KafkaWriter s) where
  pure = \a -> K $ \_ ix# s# -> (# s#, ix#, a #)
  {-# inline pure #-}
  K f <*> K g = K $ \marr# ix0# s0# -> case f marr# ix0# s0# of
    (# s1#, ix1#, h #) -> case g marr# ix1# s1# of
       (# s2#, ix2#, x #) -> (# s2#, ix2#, h x #)
  {-# inline (<*>) #-}

instance Monad (KafkaWriter s) where
  K f >>= k = K $ \marr# ix0# s0# -> case f marr# ix0# s0# of
    (# s1#, ix1#, a #) -> case k a of
      K g -> g marr# ix1# s1#
  {-# inline (>>=) #-}

ask :: KafkaWriter s (MutableByteArray s)
ask = K $ \marr# ix0# s0# -> (# s0#, ix0#, MutableByteArray marr# #)
{-# inline ask #-}

get :: KafkaWriter s Int
get = K $ \_ ix0# s0# -> (# s0#, ix0#, I# ix0# #)
{-# inline get #-}

modify :: (Int -> Int) -> KafkaWriter s ()
modify f = K $ \_ ix0# s0# ->
  let
    !(I# ix1#) = f (I# ix0#)
  in
    (# s0#, ix1#, () #)
{-# inline modify #-}

instance PrimMonad (KafkaWriter s) where
  type PrimState (KafkaWriter s) = s
  primitive f = K $ \_ ix0# s0# -> case f s0# of
    (# s1#, a #) -> (# s1#, ix0#, a #)
  {-# inline primitive #-}

instance Semigroup a => Semigroup (KafkaWriter s a) where
  (<>) = liftA2 (<>)
  {-# inline (<>) #-}

instance Monoid a => Monoid (KafkaWriter s a) where
  mempty = pure mempty
  {-# inline mempty #-}

withCtx :: (Int -> MutableByteArray s -> KafkaWriter s a) -> KafkaWriter s a
withCtx f = do
  index <- get
  arr <- ask
  f index arr
{-# inline withCtx #-}

writeNum :: (Prim a, PrimUnaligned a)
  => a -> KafkaWriter s ()
writeNum n = withCtx $ \index arr -> do
  writeUnalignedByteArray arr index n
  modify (+ (alignment n))
{-# inlineable writeNum #-}

write8 :: Int8 -> KafkaWriter s ()
write8 = writeNum

write16 :: Int16 -> KafkaWriter s ()
write16 = writeNum . toBE16

write32 :: Int32 -> KafkaWriter s ()
write32 = writeNum . toBE32

write64 :: Int64 -> KafkaWriter s ()
write64 = writeNum . toBE64

build8 :: Int8 -> KafkaWriterBuilder s
build8 i = Kwb 1 (write8 i)

build16 :: Int16 -> KafkaWriterBuilder s
build16 i = Kwb 2 (write16 i)

build32 :: Int32 -> KafkaWriterBuilder s
build32 i = Kwb 4 (write32 i)

build64 :: Int64 -> KafkaWriterBuilder s
build64 i = Kwb 8 (write64 i)

buildBool :: Bool -> KafkaWriterBuilder s
buildBool b = Kwb 1 (write8 (if b then 1 else 0))

writeBytes ::
     ByteArray
  -> Int
  -> KafkaWriter s ()
writeBytes src len = withCtx $ \index arr -> do
  copyByteArray arr index src 0 len
  modify (+len)

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

runKafkaWriter# :: Int# -> KafkaWriter s a -> State# s -> (# State# s, ByteArray# #)
runKafkaWriter# sz# (K g) = \s0# -> case newByteArray# sz# s0# of
  (# s1#, marr# #) -> case g marr# 0# s1# of
    (# s2#, _, _ #) -> case unsafeFreezeByteArray# marr# s2# of
      (# s3#, b# #) -> (# s3#, b# #)
{-# inline runKafkaWriter# #-}

data KafkaWriterBuilder s = Kwb
  !Int -- ^ length
  !(KafkaWriter s ())

instance Semigroup (KafkaWriterBuilder s) where
  Kwb len1 x <> Kwb len2 y = Kwb (len1 + len2) (x <> y)
  {-# inline (<>) #-}

instance Monoid (KafkaWriterBuilder s) where
  mempty = Kwb 0 mempty
  {-# inline mempty #-}

toBE16 :: Int16 -> Int16
toBE16 = fromIntegral . byteSwap16 . fromIntegral

toBE32 :: Int32 -> Int32
toBE32 = fromIntegral . byteSwap32 . fromIntegral

toBE64 :: Int64 -> Int64
toBE64 = fromIntegral . byteSwap64 . fromIntegral

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

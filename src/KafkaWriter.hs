{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE UndecidableInstances #-}

module KafkaWriter
  ( KafkaWriter(..)
  , evaluateWriter
  , withCtx
  , writeNum
  , write8
  , write16
  , write32
  , write64
  , writeArray
  , build8
  , build16
  , build32
  , build64
  , buildArray

  , evaluate
  , foldBuilder
  ) where

import Control.Applicative (liftA2)
import Control.Monad.Primitive
import Control.Monad.Reader
import Control.Monad.ST
import Control.Monad.State.Strict
import Data.Int
import Data.Foldable (fold)
import Data.Functor.Reverse (Reverse(..))
import Data.Primitive (Prim(..), alignment)
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned

import Common (toBE16, toBE32, toBE64)

newtype KafkaWriter s a = KafkaWriter
  { runKafkaWriter :: ReaderT (MutableByteArray s) (StateT Int (ST s)) a }
  deriving
    ( Functor, Applicative, Monad
    , MonadReader (MutableByteArray s)
    , MonadState Int
    , PrimMonad
    )

instance Semigroup a => Semigroup (KafkaWriter s a) where
  (<>) = liftA2 (<>)

instance Monoid a => Monoid (KafkaWriter s a) where
  mempty = pure mempty

withCtx :: (Int -> MutableByteArray s -> KafkaWriter s a) -> KafkaWriter s a
withCtx f = do
  index <- get
  arr <- ask
  f index arr

writeNum :: (Prim a, PrimUnaligned a)
  => a -> KafkaWriter s ()
writeNum n = withCtx $ \index arr -> do
  writeUnalignedByteArray arr index n
  modify' (+ (alignment n))
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

writeArray ::
     ByteArray
  -> Int
  -> KafkaWriter s ()
writeArray src len = withCtx $ \index arr -> do
  copyByteArray arr index src 0 len
  modify' (+len)

buildArray :: ByteArray -> Int -> KafkaWriterBuilder s
buildArray src len = Kwb len (writeArray src len)

evaluateWriter :: Int -> (forall s. KafkaWriter s a) -> ByteArray
evaluateWriter n kw = runST $ do
  arr <- newByteArray n
  _ <- runStateT (runReaderT (runKafkaWriter kw) arr) 0
  unsafeFreezeByteArray arr

evaluate :: (forall s. KafkaWriterBuilder s) -> ByteArray
evaluate kwb = runST (go kwb)
  where
    go :: forall s. KafkaWriterBuilder s -> ST s ByteArray
    go (Kwb len kw) = do
      arr <- newByteArray len
      void $ runStateT (runReaderT (runKafkaWriter kw) arr) 0
      unsafeFreezeByteArray arr

foldBuilder :: Foldable t => t (KafkaWriterBuilder s) -> KafkaWriterBuilder s
foldBuilder = fold . Reverse

data KafkaWriterBuilder s = Kwb
  !Int -- ^ length
  !(KafkaWriter s ())

instance Semigroup (KafkaWriterBuilder s) where
  Kwb len1 x <> Kwb len2 y = Kwb (len1 + len2) (x <> y)

instance Monoid (KafkaWriterBuilder s) where
  mempty = Kwb 0 mempty

--kwb :: Int -> KafkaWriter s () -> KafkaWriter s (KafkaWriterBuilder s)
--kwb !len writer =


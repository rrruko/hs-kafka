{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE UndecidableInstances #-}

module KafkaWriter
  ( KafkaWriter(..)
  , evaluateWriter
  , withCtx
  , writeNum
  , write8
  , writeBE16
  , writeBE32
  , writeBE64
  , writeArray
  ) where

import Control.Monad.Primitive
import Control.Monad.Reader
import Control.Monad.ST
import Control.Monad.State.Strict
import Data.Int
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

writeBE16 :: Int16 -> KafkaWriter s ()
writeBE16 = writeNum . toBE16

writeBE32 :: Int32 -> KafkaWriter s ()
writeBE32 = writeNum . toBE32

writeBE64 :: Int64 -> KafkaWriter s ()
writeBE64 = writeNum . toBE64

writeArray ::
     ByteArray
  -> Int
  -> KafkaWriter s ()
writeArray src len = withCtx $ \index arr -> do
  copyByteArray arr index src 0 len
  modify' (+len)

evaluateWriter :: Int -> (forall s. KafkaWriter s a) -> ByteArray
evaluateWriter n kw = runST $ do
  arr <- newByteArray n
  _ <- runStateT (runReaderT (runKafkaWriter kw) arr) 0
  unsafeFreezeByteArray arr

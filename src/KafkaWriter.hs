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
import Control.Monad.Primitive.Convenience
import Control.Monad.Reader
import Control.Monad.ST
import Control.Monad.State.Strict
import Data.Int
import Data.Primitive (Prim(..), alignment)
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned

import Common (toBE16, toBE32, toBE64)

newtype KafkaWriter s m a = KafkaWriter
  { runKafkaWriter :: ReaderT (MutableByteArray s) (StateT Int m) a }
  deriving
    ( Functor, Applicative, Monad
    , MonadReader (MutableByteArray s)
    , MonadState Int
    , PrimMonad
    )

withCtx :: Monad m => (Int -> MutableByteArray s -> KafkaWriter s m a) -> KafkaWriter s m a
withCtx f = do
  index <- get
  arr <- ask
  f index arr

writeNum :: (MonadPrim s m, Prim a, PrimUnaligned a)
  => a -> KafkaWriter s m ()
writeNum n = withCtx $ \index arr -> do
  writeUnalignedByteArray arr index n
  modify' (+ (alignment n))
{-# inlineable writeNum #-}

write8 :: (MonadPrim s m) => Int8 -> KafkaWriter s m ()
write8 = writeNum

writeBE16 :: (MonadPrim s m) => Int16 -> KafkaWriter s m ()
writeBE16 = writeNum . toBE16

writeBE32 :: (MonadPrim s m) => Int32 -> KafkaWriter s m ()
writeBE32 = writeNum . toBE32

writeBE64 :: (MonadPrim s m) => Int64 -> KafkaWriter s m ()
writeBE64 = writeNum . toBE64

writeArray ::
     (MonadPrim s m)
  => ByteArray
  -> Int
  -> KafkaWriter s m ()
writeArray src len = withCtx $ \index arr -> do
  copyByteArray arr index src 0 len
  modify' (+len)

evaluateWriter :: Int -> (forall s. KafkaWriter s (ST s) a) -> ByteArray
evaluateWriter n kw = runST $ do
  arr <- newByteArray n
  _ <- runStateT (runReaderT (runKafkaWriter kw) arr) 0
  unsafeFreezeByteArray arr

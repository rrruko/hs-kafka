{-# language
        BangPatterns
      , MagicHash
      , RankNTypes
      , UnboxedTuples
  #-}

{-# options_ghc -fno-warn-orphans #-}

module Main (main) where

import Control.Applicative (liftA2)
import Control.Monad (replicateM_)
import GHC.Exts
import Unsafe.Coerce (unsafeCoerce)

import Hedgehog
import Hedgehog.Classes
--import qualified Hedgehog.Gen as Gen
--import qualified Hedgehog.Range as Range

import Kafka.Internal.Writer hiding (KafkaWriterBuilder(..))

main :: IO Bool
main = lawsCheckMany
  [ ( "KafkaWriter s"
    , [ functorLaws genKw
      , applicativeLaws genKw
      , monadLaws genKw
      ]
    )
  ]

-- | We assume a size of 1000.
genKw :: Gen a -> Gen (KafkaWriter s a)
genKw genA = do
  a <- genA
  pure $ do
    replicateM_ 200 (write8 7)
    replicateM_ 100 (write16 11)
    replicateM_ 50 (write32 13)
    replicateM_ 25 (write64 17)
    pure a

instance Eq a => Eq (KafkaWriter s a) where
  (==) = reallyUnsafeKafkaWriterEquality
  {-# inline (==) #-}

instance Show a => Show (KafkaWriter s a) where
  show = unsafeShowKafkaWriter
  {-# inline show #-}

unsafeShowKafkaWriter :: forall s a. Show a
  => KafkaWriter s a
  -> String
unsafeShowKafkaWriter kw = run (unsafeCoerce $ fmap show kw)
{-# noinline unsafeShowKafkaWriter #-}

reallyUnsafeKafkaWriterEquality :: (Eq a)
  => KafkaWriter s a
  -> KafkaWriter s a
  -> Bool
reallyUnsafeKafkaWriterEquality kw1 kw2 = run
  (unsafeCoerce $ liftA2 (==) kw1 kw2)
{-# noinline reallyUnsafeKafkaWriterEquality #-}

run :: (forall s. KafkaWriter s a) -> a
run kw = case runRW# (runKafkaWriter# 10000# kw) of
  (# _, a #) -> a

runKafkaWriter# :: Int# -> KafkaWriter s a -> State# s -> (# State# s, a #)
runKafkaWriter# sz# (K g) = \s0# -> case newByteArray# sz# s0# of
  (# s1#, marr# #) -> case g marr# 0# s1# of
    (# s2#, _, a #) -> (# s2#, a #)
{-# inline runKafkaWriter# #-}

data KafkaWriterBuilder s a = Kwb
  !Int -- ^ length
  !(KafkaWriter s a)

instance Semigroup a => Semigroup (KafkaWriterBuilder s a) where
  Kwb len1 x <> Kwb len2 y = Kwb (len1 + len2) (x <> y)
  {-# inline (<>) #-}

instance Monoid a => Monoid (KafkaWriterBuilder s a) where
  mempty = Kwb 0 mempty
  {-# inline mempty #-}


{-# language
        BangPatterns
      , MagicHash
      , RankNTypes
      , UnboxedTuples
  #-}

{-# options_ghc -fno-warn-orphans #-}

module Main (main) where

import Unsafe.Coerce (unsafeCoerce)

import Hedgehog
import Hedgehog.Classes
--import qualified Hedgehog.Gen as Gen
--import qualified Hedgehog.Range as Range

import Kafka.Internal.Writer

main :: IO Bool
main = lawsCheckMany
  [ ( "KafkaWriterBuilder s"
    , [ semigroupLaws genKw
      , monoidLaws genKw
      ]
    )
  ]

defaultKwb :: KafkaWriterBuilder s
defaultKwb = mconcat (
     replicate 200 (build8 7)
  ++ replicate 100 (build16 11)
  ++ replicate 50 (build32 13)
  ++ replicate 25 (build64 17))

-- | We assume a size of 1000.
genKw :: Gen (KafkaWriterBuilder s)
genKw = pure defaultKwb

instance Eq (KafkaWriterBuilder s) where
  (==) = reallyUnsafeKafkaWriterEquality

instance Show (KafkaWriterBuilder s) where
  show _ = "<KafkaWriterBuilder>"

reallyUnsafeKafkaWriterEquality :: ()
  => KafkaWriterBuilder s
  -> KafkaWriterBuilder s
  -> Bool
reallyUnsafeKafkaWriterEquality kw1 kw2 =
  evaluate (unsafeCoerce kw1) == evaluate (unsafeCoerce kw2)
{-# noinline reallyUnsafeKafkaWriterEquality #-}

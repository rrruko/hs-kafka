{-# language
        BangPatterns
      , MagicHash
      , RankNTypes
      , UnboxedTuples
  #-}

{-# options_ghc -fno-warn-orphans #-}

module Main (main) where

import Hedgehog
import Hedgehog.Classes
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import Kafka.Internal.Writer

main :: IO Bool
main = lawsCheckMany
  [ ( "Builder"
    , [ semigroupLaws genBuilder
      , monoidLaws genBuilder
      ]
    )
  ]

genLen :: Gen Int
genLen = Gen.int (Range.linear 25 200)

builders :: [Gen Builder]
builders = map pure [ int8 7, int16 11, int32 13, int64 17 ]

genSmallBuilder :: Gen Builder
genSmallBuilder = do
  len <- genLen
  b <- Gen.choice builders
  pure $ mconcat (replicate len b)

genBuilder :: Gen Builder
genBuilder = do
  a <- genSmallBuilder
  b <- genSmallBuilder
  c <- genSmallBuilder
  d <- genSmallBuilder
  pure (a <> b <> c <> d)

instance Eq Builder where
  (==) = reallyUnsafeKafkaWriterEquality

instance Show Builder where
  show _ = "<Builder>"

reallyUnsafeKafkaWriterEquality :: ()
  => Builder
  -> Builder
  -> Bool
reallyUnsafeKafkaWriterEquality b1 b2 =
  build b1 == build b2
{-# noinline reallyUnsafeKafkaWriterEquality #-}

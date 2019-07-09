{-# LANGUAGE OverloadedStrings #-}

import Data.Foldable
import Data.IORef
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Test.Tasty
import Test.Tasty.HUnit

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

import Varint

main :: IO ()
main = defaultMain unitTests

unitTests :: TestTree
unitTests = testGroup "unit tests"
  [ testCase
      "zigzag 0 is 0"
      (assertBool $ zigzag 0 == 0)
  , testCase
      "zigzag -1 is 1"
      (assertBool $ zigzag (-1) == 1)
  , testCase
      "zigzag 1 is 2"
      (assertBool $ zigzag 1 == 2)
  , testCase
      "zigzag -2 is 3"
      (assertBool $ zigzag (-2) == 3)
  ]

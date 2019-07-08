{-# LANGUAGE OverloadedStrings #-}

import Data.Foldable
import Data.IORef
import Data.Primitive.ByteArray
import Test.Tasty
import Test.Tasty.Ingredients.ConsoleReporter
import Test.Tasty.Golden

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

import Kafka

main :: IO ()
main = defaultMain goldenTests

goldenTests :: TestTree
goldenTests = testGroup "golden tests"
  [ goldenVsString
      "generate well-formed requests"
      "test/kafka-request-bytes"
      (BL.fromStrict <$> requestTest)
  ]

requestTest = do
  ref <- newIORef 0
  let payload = fromByteString $
        "\"im not owned! im not owned!!\", i continue to insist as i slowly" <>
        "shrink and transform into a corn cob"
      topicName = fromByteString "test"
      topic = Topic topicName 1 ref
      req = toByteString $ produceRequest 30000 topic payload
  pure req

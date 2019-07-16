{-# LANGUAGE OverloadedStrings #-}

import Data.ByteString (ByteString)
import Data.Foldable
import Data.IORef
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Data.Word
import Test.Tasty
import Test.Tasty.Ingredients.ConsoleReporter
import Test.Tasty.Golden
import Test.Tasty.HUnit

import qualified Data.Attoparsec.ByteString as AT
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

import Kafka
import Kafka.Combinator
import Kafka.Common
import Kafka.Fetch.Request
import Kafka.Produce.Request
import Kafka.Produce.Response
import Kafka.Varint

main :: IO ()
main = defaultMain (testGroup "Tests" [unitTests, goldenTests])

unitTests :: TestTree
unitTests = testGroup "Unit tests" [zigzagTests, parserTests]

zigzagTests :: TestTree
zigzagTests = testGroup "zigzag"
  [ testCase
      "zigzag 0 is 0"
      (zigzag 0 @=? byteArrayFromList [0 :: Word8])
  , testCase
      "zigzag -1 is 1"
      (zigzag (-1) @=? byteArrayFromList [1 :: Word8])
  , testCase
      "zigzag 1 is 2"
      (zigzag 1 @=? byteArrayFromList [2 :: Word8])
  , testCase
      "zigzag -2 is 3"
      (zigzag (-2) @=? byteArrayFromList [3 :: Word8])
  , testCase
      "zigzag 100 is [200, 1]"
      (zigzag 100 @?= byteArrayFromList [200, 1 :: Word8])
  , testCase
      "zigzag 150 is [172, 2]"
      (zigzag 150 @?= byteArrayFromList [172, 2 :: Word8])
  ]

parserTests :: TestTree
parserTests = testGroup "Parsers"
  [ testCase
      "int32 [0, 0, 0, 255] is 255"
      (AT.parseOnly int32 (B.pack [0,0,0,255]) @?= Right 255)
  , testCase
      "int32 [0x12, 0x34, 0x56, 0x78] is 305419896"
      (AT.parseOnly int32 (B.pack [0x12, 0x34, 0x56, 0x78]) @?= Right 305419896)
  , testCase
      "parseVarint (zigzag 0) is 0"
      (AT.parseOnly parseVarint (toByteString $ zigzag 0) @?= Right 0)
  , testCase
      "parseVarint (zigzag 10) is 10"
      (AT.parseOnly parseVarint (toByteString $ zigzag 10) @?= Right 10)
  , testCase
      "parseVarint (zigzag 150) is 150"
      (AT.parseOnly parseVarint (toByteString $ zigzag 150) @?= Right 150)
  , testCase
      "parseVarint (zigzag 1000) is 1000"
      (AT.parseOnly parseVarint (toByteString $ zigzag 1000) @?= Right 1000)

  ]

goldenTests :: TestTree
goldenTests = testGroup "Golden tests"
  [ goldenVsString
      "generate well-formed produce request for a single payload"
      "test/single-produce-request"
      (BL.fromStrict <$> produceTest)
  , goldenVsString
      "generate well-formed produce request for many payloads"
      "test/multiple-produce-request"
      (BL.fromStrict <$> multipleProduceTest)
  , goldenVsString
      "generate well-formed fetch request for a single partition"
      "test/fetch-request"
      (BL.fromStrict <$> fetchTest)
  ]

unChunks :: UnliftedArray ByteArray -> ByteArray
unChunks = foldrUnliftedArray (<>) mempty

produceTest :: IO ByteString
produceTest = do
  ref <- newIORef 0
  let payload = fromByteString $
        "\"im not owned! im not owned!!\", i continue to insist as i slowly" <>
        "shrink and transform into a corn cob"
  payloads <- newUnliftedArray 1 payload
  payloadsf <- freezeUnliftedArray payloads 0 1
  let topicName = fromByteString "test"
      topic = Topic topicName 1 ref
      req = toByteString $ unChunks $ produceRequest 30000 topic 0 payloadsf
  pure req

multipleProduceTest :: IO ByteString
multipleProduceTest = do
  ref <- newIORef 0
  let payloads = unliftedArrayFromList
        [ fromByteString "i'm dying"
        , fromByteString "is it blissful?"
        , fromByteString "it's like a dream"
        , fromByteString "i want to dream"
        ]
  let topicName = fromByteString "test"
      topic = Topic topicName 1 ref
      req = toByteString $ unChunks $ produceRequest 30000 topic 0 payloads
  pure req

fetchTest :: IO ByteString
fetchTest = do
  ref <- newIORef 0
  let topicName = fromByteString "test"
      topic = Topic topicName 1 ref
      req = toByteString $ unChunks $
        sessionlessFetchRequest 30000 topic [Partition 0 0]
  pure req

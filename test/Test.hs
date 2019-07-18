{-# LANGUAGE OverloadedStrings #-}

import Data.ByteString (ByteString)
import Data.Foldable
import Data.Int
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
import Kafka.ListOffsets.Request
import Kafka.Produce.Request
import Kafka.Produce.Response
import Kafka.Writer
import Kafka.Varint

main :: IO ()
main = defaultMain (testGroup "Tests" [unitTests, goldenTests])

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [ zigzagTests
  , parserTests
  , responseParserTests
  ]

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

responseParserTests :: TestTree
responseParserTests = testGroup "Response parsers"
  [ produceResponseTest
  ]

goldenTests :: TestTree
goldenTests = testGroup "Golden tests"
  [ testGroup "Produce"
      [ goldenVsString
          "One payload"
          "test/produce-one-payload-request"
          (BL.fromStrict <$> produceTest)
      , goldenVsString
          "Many payloads"
          "test/produce-many-payloads-request"
          (BL.fromStrict <$> multipleProduceTest)
      ]
  , testGroup "Fetch"
      [ goldenVsString
          "One partition"
          "test/fetch-one-partition-request"
          (BL.fromStrict <$> fetchTest)
      , goldenVsString
          "Many partitions"
          "test/fetch-many-partitions-request"
          (BL.fromStrict <$> multipleFetchTest)

      ]
  , testGroup "ListOffsets"
      [ goldenVsString
          "No partitions"
          "test/listoffsets-no-partitions-request"
          (BL.fromStrict <$> listOffsetsTest [])
      , goldenVsString
          "One partition"
          "test/listoffsets-one-partition-request"
          (BL.fromStrict <$> listOffsetsTest [0])
      , goldenVsString
          "Many partitions"
          "test/listoffsets-many-partitions-request"
          (BL.fromStrict <$> listOffsetsTest [0,1,2,3,4,5])
      ]
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

multipleFetchTest :: IO ByteString
multipleFetchTest = do
  ref <- newIORef 0
  let topicName = fromByteString "test"
      topic = Topic topicName 1 ref
      req = toByteString $ unChunks $
        sessionlessFetchRequest 30000 topic
          [Partition 0 0, Partition 1 0, Partition 2 0]
  pure req

listOffsetsTest :: [Int32] -> IO ByteString
listOffsetsTest partitions = do
  ref <- newIORef 0
  let topicName = fromByteString "test"
      topic = Topic topicName 1 ref
      req = toByteString $ unChunks $
        listOffsetsRequest topic partitions
  pure req

produceResponseTest :: TestTree
produceResponseTest = testGroup "Produce"
  [ testCase
      "One message"
      (parseProduce oneMsgProduceResponseBytes @?=
        Right oneMsgProduceResponse)
  , testCase
      "Two messages"
      (parseProduce twoMsgProduceResponseBytes @?=
        Right twoMsgProduceResponse)
  ]

parseProduce :: ByteArray -> Either String ProduceResponse
parseProduce ba = AT.parseOnly parseProduceResponse (toByteString ba)

oneMsgProduceResponseBytes :: ByteArray
oneMsgProduceResponseBytes = evaluate $ foldBuilder
  [ build32 0
  , build32 1
  , buildString (fromByteString "topic-name") 10
  , build32 1
  , build32 10
  , build16 11
  , build64 12
  , build64 13
  , build64 14
  , build32 1
  ]

twoMsgProduceResponseBytes :: ByteArray
twoMsgProduceResponseBytes = evaluate $ foldBuilder
  [ build32 0
  , build32 1
  , buildString (fromByteString "topic-name") 10
  , build32 2
  , build32 10
  , build16 11
  , build64 12
  , build64 13
  , build64 14
  , build32 20
  , build16 21
  , build64 22
  , build64 23
  , build64 24
  , build32 1
  ]

oneMsgProduceResponse :: ProduceResponse
oneMsgProduceResponse =
  ProduceResponse
    { produceResponseMessages =
        [ ProduceResponseMessage
            { prMessageTopic = "topic-name"
            , prPartitionResponses =
                [ ProducePartitionResponse
                    { prResponsePartition = 10
                    , prResponseErrorCode = 11
                    , prResponseBaseOffset = 12
                    , prResponseLogAppendTime = 13
                    , prResponseLogStartTime = 14
                    }
                ]
            }
        ]
    , throttleTimeMs = 1
    }

twoMsgProduceResponse :: ProduceResponse
twoMsgProduceResponse =
  ProduceResponse
    { produceResponseMessages =
        [ ProduceResponseMessage
            { prMessageTopic = "topic-name"
            , prPartitionResponses =
                [ ProducePartitionResponse
                    { prResponsePartition = 10
                    , prResponseErrorCode = 11
                    , prResponseBaseOffset = 12
                    , prResponseLogAppendTime = 13
                    , prResponseLogStartTime = 14
                    }
                , ProducePartitionResponse
                    { prResponsePartition = 20
                    , prResponseErrorCode = 21
                    , prResponseBaseOffset = 22
                    , prResponseLogAppendTime = 23
                    , prResponseLogStartTime = 24
                    }
                ]
            }
        ]
    , throttleTimeMs = 1
    }

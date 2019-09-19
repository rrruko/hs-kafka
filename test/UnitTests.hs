{-# LANGUAGE OverloadedStrings #-}

{-# options_ghc -Wwarn #-}

module Main (main) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Data.Word
import Data.String (fromString)
import Test.Tasty
import Test.Tasty.Golden
import Test.Tasty.HUnit
import Prelude hiding (readFile)
import Data.ByteString (ByteString)

import qualified Data.List as L
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC8
import qualified Data.ByteString.Lazy as BL
import qualified Data.Bytes.Parser as Smith
import qualified String.Ascii as S
import qualified Data.IntMap as IM
import qualified System.IO as IO

import Data.Bytes.Parser (Result(..))

import Kafka.Consumer (merge)
import Kafka.Internal.Combinator
import Kafka.Internal.Fetch.Request
import Kafka.Internal.JoinGroup.Request
import Kafka.Internal.ListOffsets.Request
import Kafka.Internal.Produce.Request
import Kafka.Internal.Produce.Response
import qualified Kafka.Internal.Writer as W
import Kafka.Internal.Zigzag

import qualified Kafka.Internal.Fetch.Response as Fetch

main :: IO ()
main = defaultMain (testGroup "Tests" [unitTests, goldenTests])

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [ zigzagTests
  , parserTests
  , responseParserTests
  , consumerTests
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

readFile :: FilePath -> IO S.String
readFile fp = do
  b <- B.readFile fp
  pure (S.unsafeFromByteArray (fromByteString b))

fromByteString :: ByteString -> ByteArray
fromByteString = byteArrayFromList . B.unpack

toByteString :: ByteArray -> ByteString
toByteString = B.pack . foldrByteArray (:) []

parserTests :: TestTree
parserTests = testGroup "Parsers"
  [ testCase
      "int32 [0, 0, 0, 255] is 255"
      (Smith.parseByteArray (int32 "") (byteArrayFromList [0,0,0,255 :: Word8]) @?= Success 255 0)
  , testCase
      "int32 [0x12, 0x34, 0x56, 0x78] is 305419896"
      (Smith.parseByteArray (int32 "") (byteArrayFromList [0x12, 0x34, 0x56, 0x78 :: Int8]) @?= Success 305419896 0)
  , testCase
      "parseVarint (zigzag 0) is 0"
      (Smith.parseByteArray varInt (zigzag 0) @?= Success 0 0)
  , testCase
      "parseVarint (zigzag 10) is 10"
      (Smith.parseByteArray varInt (zigzag 10) @?= Success 10 0)
  , testCase
      "parseVarint (zigzag 150) is 150"
      (Smith.parseByteArray varInt (zigzag 150) @?= Success 150 0)
  , testCase
      "parseVarint (zigzag 1000) is 1000"
      (Smith.parseByteArray varInt (zigzag 1000) @?= Success 1000 0)
  , testCase
      "parseVarint (zigzag (-1)) is (-1)"
      (Smith.parseByteArray varInt (zigzag (-1)) @?= Success (-1) 0)
  ]

responseParserTests :: TestTree
responseParserTests = testGroup "Response parsers"
  [ produceResponseTest
  , fetchResponseTest
  ]

goldenTests :: TestTree
goldenTests = testGroup "Golden tests"
  [ testGroup "Produce"
      [ goldenVsString
          "One payload"
          "test/golden/produce-one-payload-request"
          (BL.fromStrict <$> produceTest)
      , goldenVsString
          "Many payloads"
          "test/golden/produce-many-payloads-request"
          (BL.fromStrict <$> multipleProduceTest)
      ]
  ]
{-
  , testGroup "Fetch"
      [ goldenVsString
          "One partition"
          "test/golden/fetch-one-partition-request"
          (BL.fromStrict <$> fetchTest)
      , goldenVsString
          "Many partitions"
          "test/golden/fetch-many-partitions-request"
          (BL.fromStrict <$> multipleFetchTest)
      ]
  , testGroup "ListOffsets"
      [ goldenVsString
          "No partitions"
          "test/golden/listoffsets-no-partitions-request"
          (BL.fromStrict <$> listOffsetsTest [])
      , goldenVsString
          "One partition"
          "test/golden/listoffsets-one-partition-request"
          (BL.fromStrict <$> listOffsetsTest [0])
      , goldenVsString
          "Many partitions"
          "test/golden/listoffsets-many-partitions-request"
          (BL.fromStrict <$> listOffsetsTest [0,1,2,3,4,5])
      ]
  , testGroup "JoinGroup"
      [ goldenVsString
          "null member id"
          "test/golden/joingroup-null-member-id-request"
          (BL.fromStrict <$> joinGroupTest
            (GroupMember (fromByteString "test-group") Nothing))
      , goldenVsString
          "with member id"
          "test/golden/joingroup-with-member-id-request"
          (BL.fromStrict <$> joinGroupTest
            (GroupMember
              (fromByteString "test-group")
              (Just $ fromByteString "test-member-id")))
      ]
  ]
-}

produceTest :: IO ByteString
produceTest = do
  let payload = fromByteString $ "\"im not owned! im not owned!!\", i continue to insist as i slowly shrink and transform into a corn cob"
  payloads <- do
    payloads <- newUnliftedArray 1 payload
    freezeUnliftedArray payloads 0 1
  let got = toByteString
        . unChunks
        $ produceRequest 30000 "test" 0 payloads
  --expected <- B.readFile "test/golden/produce-one-payload-request"
  pure got

unChunks :: UnliftedArray ByteArray -> ByteArray
unChunks = foldrUnliftedArray (<>) mempty

multipleProduceTest :: IO ByteString
multipleProduceTest = do
  let payloads = unliftedArrayFromList
        [ fromByteString "i'm dying"
        , fromByteString "is it blissful?"
        , fromByteString "it's like a dream"
        , fromByteString "i want to dream"
        ]
  let req = toByteString
        . unChunks
        $ produceRequest 30000 "test" 0 payloads
  pure req


{-
fetchTest :: IO ByteString
fetchTest = do
  let topicName = fromByteString "test"
      req = toByteString $ unChunks $
        sessionlessFetchRequest 30000 (TopicName topicName) [PartitionOffset 0 0] 30000000
  pure req

multipleFetchTest :: IO ByteString
multipleFetchTest = do
  let topicName = fromByteString "test"
      req = toByteString $ unChunks $
        sessionlessFetchRequest 30000 (TopicName topicName)
          [PartitionOffset 0 0, PartitionOffset 1 0, PartitionOffset 2 0] 30000000
  pure req

listOffsetsTest :: [Int32] -> IO ByteArray
listOffsetsTest partitions = do
  let req = unChunks (listOffsetsRequest "test" partitions Latest)
  pure req

joinGroupTest :: GroupMember -> IO ByteArray
joinGroupTest groupMember = do
  let req = unChunks (joinGroupRequest "test" groupMember)
  pure req
-}

produceResponseTest :: TestTree
produceResponseTest = testGroup "Produce"
  [ testCase
      "One message"
      (parseProduce oneMsgProduceResponseBytes @?=
        Success oneMsgProduceResponse 0)
  , testCase
      "Two messages"
      (parseProduce twoMsgProduceResponseBytes @?=
        Success twoMsgProduceResponse 0)
  ]

parseProduce :: ByteArray -> Result String ProduceResponse
parseProduce = Smith.parseByteArray parseProduceResponse

oneMsgProduceResponseBytes :: ByteArray
oneMsgProduceResponseBytes = W.build $
  W.int32 0
  <> W.int32 1
  <> W.string "topic-name" 10
  <> W.int32 1
  <> W.int32 10
  <> W.int16 11
  <> W.int64 12
  <> W.int64 13
  <> W.int64 14
  <> W.int32 1

twoMsgProduceResponseBytes :: ByteArray
twoMsgProduceResponseBytes = W.build $
  W.int32 0
  <> W.int32 1
  <> W.string "topic-name" 10
  <> W.int32 2
  <> W.int32 10
  <> W.int16 11
  <> W.int64 12
  <> W.int64 13
  <> W.int64 14
  <> W.int32 20
  <> W.int16 21
  <> W.int64 22
  <> W.int64 23
  <> W.int64 24
  <> W.int32 1

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

fetchResponseTest :: TestTree
fetchResponseTest = testGroup "Fetch"
  [ goldenVsString
      "Many batches"
      "test/golden/fetch-response-parsed"
      (do
        bytes <- readFile "test/golden/fetch-response-bytes"
        case Smith.parseByteArray Fetch.parseFetchResponse (S.toByteArray bytes) of
          Failure e -> fail ("Parse failed with " <> e)
          Success res _n -> pure (BL.fromStrict (BC8.pack (show res)))
      )
  ]

consumerTests :: TestTree
consumerTests = testGroup "Consumer"
  [ testCase
      "merge replaces -1 and keeps other values"
      mergeTest
  ]
  where
  mergeTest =
    let actual =
          merge
            (IM.fromList [(0, 5), (1, 6), (2, 7)])
            (IM.fromList [(0, -1), (1, 3), (2, -1)])
    in  actual @=? (IM.fromList [(0, 5), (1, 3), (2, 7)])

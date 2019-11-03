{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Int
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Data.Word
import Test.Tasty
import Test.Tasty.Golden
import Test.Tasty.HUnit
import Prelude hiding (readFile)
import Data.ByteString (ByteString)

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC8
import qualified Data.ByteString.Lazy as BL
import qualified Data.Bytes.Parser as Smith
import qualified String.Ascii as S
import qualified Data.IntMap as IM

import Data.Bytes.Parser (Result(..))

import Kafka.Common
import Kafka.Consumer (merge)
import Kafka.Internal.Combinator
import Kafka.Internal.Fetch.Request
import Kafka.Internal.JoinGroup.Request
import Kafka.Internal.ListOffsets.Request
import Kafka.Internal.Produce.Request
import Kafka.Internal.Produce.Response
import Kafka.Internal.Zigzag
import qualified Kafka.Internal.Fetch.Response as Fetch
import qualified Kafka.Internal.Writer as W

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
      (Smith.parseByteArray (int32 "") (byteArrayFromList [0,0,0,255 :: Word8]) @?= Success (Smith.Slice 4 0 255))
  , testCase
      "int32 [0x12, 0x34, 0x56, 0x78] is 305419896"
      (Smith.parseByteArray (int32 "") (byteArrayFromList [0x12, 0x34, 0x56, 0x78 :: Int8]) @?= Success (Smith.Slice 4 0 305419896))
  , testCase
      "parseVarint (zigzag 0) is 0"
      (Smith.parseByteArray varInt (zigzag 0) @?= Success (Smith.Slice 1 0 0))
  , testCase
      "parseVarint (zigzag 10) is 10"
      (Smith.parseByteArray varInt (zigzag 10) @?= Success (Smith.Slice 1 0 10))
  , testCase
      "parseVarint (zigzag 150) is 150"
      (Smith.parseByteArray varInt (zigzag 150) @?= Success (Smith.Slice 2 0 150))
  , testCase
      "parseVarint (zigzag 1000) is 1000"
      (Smith.parseByteArray varInt (zigzag 1000) @?= Success (Smith.Slice 2 0 1000))
  , testCase
      "parseVarint (zigzag (-1)) is (-1)"
      (Smith.parseByteArray varInt (zigzag (-1)) @?= Success (Smith.Slice 1 0 (-1)))
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
          produceTest
      , goldenVsString
          "Many payloads"
          "test/golden/produce-many-payloads-request"
          multipleProduceTest
      ]
  , testGroup "Fetch"
      [ goldenVsString
          "One partition"
          "test/golden/fetch-one-partition-request"
          fetchTest
      , goldenVsString
          "Many partitions"
          "test/golden/fetch-many-partitions-request"
          multipleFetchTest
      ]
  , testGroup "ListOffsets"
      [ goldenVsString
          "No partitions"
          "test/golden/listoffsets-no-partitions-request"
          (listOffsetsTest [])
      , goldenVsString
          "One partition"
          "test/golden/listoffsets-one-partition-request"
          (listOffsetsTest [0])
      , goldenVsString
          "Many partitions"
          "test/golden/listoffsets-many-partitions-request"
          (listOffsetsTest [0,1,2,3,4,5])
      ]
  , testGroup "JoinGroup"
      [ goldenVsString
          "null member id"
          "test/golden/joingroup-null-member-id-request"
          (joinGroupTest
            (GroupMember "test-group" Nothing))
      , goldenVsString
          "with member id"
          "test/golden/joingroup-with-member-id-request"
          (joinGroupTest
            (GroupMember
              ("test-group")
              (Just $ fromByteString "test-member-id")))
      ]
  ]

toSpec :: UnliftedArray ByteArray -> BL.ByteString
toSpec = BL.fromStrict . toByteString . unChunks

produceTest :: IO BL.ByteString
produceTest = do
  let payload = fromByteString $ "\"im not owned! im not owned!!\", i continue to insist as i slowlyshrink and transform into a corn cob"
  payloads <- do
    payloads <- newUnliftedArray 1 payload
    freezeUnliftedArray payloads 0 1
  let req = produceRequest 30000 "test" 0 payloads
  pure (toSpec req)

unChunks :: UnliftedArray ByteArray -> ByteArray
unChunks = foldrUnliftedArray (<>) mempty

multipleProduceTest :: IO BL.ByteString
multipleProduceTest = do
  let payloads = unliftedArrayFromList
        [ fromByteString "i'm dying"
        , fromByteString "is it blissful?"
        , fromByteString "it's like a dream"
        , fromByteString "i want to dream"
        ]
  let req = produceRequest 30000 "test" 0 payloads
  pure (toSpec req)

fetchTest :: IO BL.ByteString
fetchTest = do
  pure (toSpec (sessionlessFetchRequest 30000 "test" [PartitionOffset 0 0] 30000000))

multipleFetchTest :: IO BL.ByteString
multipleFetchTest = do
  pure (toSpec (sessionlessFetchRequest 30000 "test" [PartitionOffset 0 0, PartitionOffset 1 0, PartitionOffset 2 0] 30000000))

listOffsetsTest :: [Int32] -> IO BL.ByteString
listOffsetsTest partitions = do
  pure (toSpec (listOffsetsRequest "test" partitions Latest))

joinGroupTest :: GroupMember -> IO BL.ByteString
joinGroupTest groupMember = do
  pure (toSpec (joinGroupRequest "test" groupMember))

produceResponseTest :: TestTree
produceResponseTest = testGroup "Produce"
  [ testCase
      "One message"
      (parseProduce oneMsgProduceResponseBytes @?=
        Success (Smith.Slice 58 0 oneMsgProduceResponse))
  , testCase
      "Two messages"
      (parseProduce twoMsgProduceResponseBytes @?=
        Success (Smith.Slice 88 0 twoMsgProduceResponse))
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
          Success (Smith.Slice _ _ res) -> pure (BL.fromStrict (BC8.pack (show res)))
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

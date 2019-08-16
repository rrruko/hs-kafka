{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

import Control.Monad.ST
import Data.Attoparsec.ByteString (parseOnly)
import Data.ByteString (ByteString)
import Data.Int
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Gauge
import System.IO.Unsafe

import qualified Data.ByteString as B

import Kafka.Common
import Kafka.Fetch.Request
import Kafka.Fetch.Response
import Kafka.Produce.Request

data ProduceArgs =
  ProduceArgs
    { pTimeout :: Int
    , pTopic :: TopicName
    , pPartition :: Int32
    , pPayloads :: UnliftedArray ByteArray
    }

produceRequest' :: ProduceArgs -> UnliftedArray ByteArray
produceRequest' (ProduceArgs {..}) =
  produceRequest pTimeout pTopic pPartition pPayloads

shortPayloads :: UnliftedArray ByteArray
shortPayloads = unliftedArrayFromList
  [ fromByteString "aaaaa"
  , fromByteString "bbbbb"
  , fromByteString "ccccc"
  , fromByteString "ddddd"
  , fromByteString "eeeee"
  ]

shortProduceArgs :: ProduceArgs
shortProduceArgs =
  ProduceArgs
    30000000
    (TopicName (fromByteString "test"))
    0
    shortPayloads

longPayloads :: UnliftedArray ByteArray
longPayloads = unliftedArrayFromList
  [ runST $ newByteArray (10*1000*1000) >>= unsafeFreezeByteArray
  ]

longProduceArgs :: ProduceArgs
longProduceArgs =
  ProduceArgs
    30000000
    (TopicName (fromByteString "test"))
    0
    longPayloads

data FetchArgs =
  FetchArgs
    { fTimeout :: Int
    , fTopic :: TopicName
    , fPartitions :: [PartitionOffset]
    }

fetchProduceArgs :: FetchArgs
fetchProduceArgs =
  FetchArgs
    30000000
    (TopicName (fromByteString "test"))
    [PartitionOffset 0 0, PartitionOffset 1 0, PartitionOffset 2 0]

fetchRequest' :: FetchArgs -> UnliftedArray ByteArray
fetchRequest' (FetchArgs {..}) =
  sessionlessFetchRequest fTimeout fTopic fPartitions 30000000

parseFetch :: ByteString -> Either String FetchResponse
parseFetch = parseOnly parseFetchResponse

fetchResponseBytes :: ByteString
{-# NOINLINE fetchResponseBytes #-}
fetchResponseBytes = unsafePerformIO $
  B.readFile "test/golden/fetch-response-bytes"

main :: IO ()
main = do
  defaultMain
    [ bgroup "produceRequest"
        [ bench "short payloads" $ whnf produceRequest' shortProduceArgs
        , bench "long payloads" $ whnf produceRequest' longProduceArgs
        ]
    , bgroup "fetchRequest"
        [ bench "fetch" $ whnf fetchRequest' fetchProduceArgs
        ]
    , bgroup "fetchResponse"
        [ bench "fetch" $ whnf parseFetch fetchResponseBytes
        ]
    ]

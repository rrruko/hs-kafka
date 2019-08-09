{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

import Control.Monad.ST
import Data.Int
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Gauge

import Kafka.Common
import Kafka.Fetch.Request
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

shortPayloads = unliftedArrayFromList
  [ fromByteString "aaaaa"
  , fromByteString "bbbbb"
  , fromByteString "ccccc"
  , fromByteString "ddddd"
  , fromByteString "eeeee"
  ]

shortProduceArgs =
  ProduceArgs
    30000000
    (TopicName (fromByteString "test"))
    0
    shortPayloads

longPayloads = unliftedArrayFromList
  [ runST $ newByteArray (10*1000*1000) >>= unsafeFreezeByteArray
  ]

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

fetchProduceArgs =
  FetchArgs
    30000000
    (TopicName (fromByteString "test"))
    [PartitionOffset 0 0, PartitionOffset 1 0, PartitionOffset 2 0]

fetchRequest' :: FetchArgs -> UnliftedArray ByteArray
fetchRequest' (FetchArgs {..}) =
  sessionlessFetchRequest fTimeout fTopic fPartitions

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
    ]

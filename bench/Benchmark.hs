{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

import Control.Monad.ST
import Data.Int
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Gauge

import Kafka.Common
import Kafka.Produce.Request

data RequestData =
  RequestData
    { timeout :: Int
    , topic :: TopicName
    , partition :: Int32
    , payloads :: UnliftedArray ByteArray
    }

produceRequest' :: RequestData -> UnliftedArray ByteArray
produceRequest' (RequestData {..}) =
  produceRequest timeout topic partition payloads

main :: IO ()
main = do
  let shortPayloads = unliftedArrayFromList
        [ fromByteString "aaaaa"
        , fromByteString "bbbbb"
        , fromByteString "ccccc"
        , fromByteString "ddddd"
        , fromByteString "eeeee"
        ]
      shortRequestData =
        RequestData
          30000000
          (TopicName (fromByteString "test"))
          0
          shortPayloads
      longPayloads = unliftedArrayFromList
        [ runST $ newByteArray (10*1000*1000) >>= unsafeFreezeByteArray
        ]
      longRequestData =
        RequestData
          30000000
          (TopicName (fromByteString "test"))
          0
          longPayloads
  defaultMain
    [ bgroup "produceRequest"
        [ bench "short payloads" $ whnf produceRequest' shortRequestData
        , bench "long payloads" $ whnf produceRequest' longRequestData
        ]
    ]

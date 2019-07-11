{-# LANGUAGE OverloadedStrings #-}

import Control.Monad.ST
import Data.IORef
import Data.ByteString
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray
import Gauge

import Common
import Kafka
import ProduceRequest

data RequestData =
  RequestData
    { timeout :: Int
    , topic :: Topic
    , payloads :: UnliftedArray ByteArray
    }

produceRequest' :: RequestData -> UnliftedArray ByteArray
produceRequest' (RequestData timeout topic payloads) =
  produceRequest timeout topic payloads

main :: IO ()
main = do
  ioref <- newIORef 0
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
          (Topic (fromByteString "test") 1 ioref)
          shortPayloads
      longPayloads = unliftedArrayFromList
        [ runST $ newByteArray (10*1000*1000) >>= unsafeFreezeByteArray
        ]
      longRequestData =
        RequestData
          30000000
          (Topic (fromByteString "test") 1 ioref)
          longPayloads
  defaultMain
    [ bgroup "produceRequest"
        [ bench "short payloads" $ whnf produceRequest' shortRequestData
        , bench "long payloads" $ whnf produceRequest' longRequestData
        ]
    ]

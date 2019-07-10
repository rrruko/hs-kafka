{-# LANGUAGE OverloadedStrings #-}

import Data.IORef
import Gauge
import Data.ByteString
import Data.Primitive.Unlifted.Array
import Data.Primitive.ByteArray

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
  let payloads = unliftedArrayFromList
        [ fromByteString "aaaaa"
        , fromByteString "bbbbb"
        , fromByteString "ccccc"
        , fromByteString "ddddd"
        , fromByteString "eeeee"
        ]
      requestData = 
        RequestData
          30000000 
          (Topic (fromByteString "test") 1 ioref) 
          payloads
  defaultMain
    [ bgroup "produceRequest"
        [ bench "produceRequest" $ whnf produceRequest' requestData
        ]
    ]

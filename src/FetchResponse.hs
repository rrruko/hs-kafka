{-# LANGUAGE LambdaCase #-}

module FetchResponse
  ( getFetchResponse
  ) where

import Data.Attoparsec.ByteString.Char8 (Parser)
import GHC.Conc

import qualified Data.Attoparsec.ByteString.Char8 as AT

import Common
import KafkaResponse

data FetchResponse = FetchResponse

parseFetchResponse :: Parser FetchResponse
parseFetchResponse = pure FetchResponse

getFetchResponse ::
     Kafka
  -> TVar Bool 
  -> IO (Either KafkaException (Either String FetchResponse))
getFetchResponse kafka interrupt =
  (fmap . fmap)
    (AT.parseOnly parseFetchResponse)
    (getKafkaResponse kafka interrupt)

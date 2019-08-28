module Kafka.Producer
  ( module ProduceResponse
  , produce
  ) where

import Kafka.Internal.Request (produce)
import Kafka.Internal.Produce.Response as ProduceResponse

{-# LANGUAGE ViewPatterns #-}

data Consumer = Consumer 
  { connection :: MVar Kafka
  , groupId :: GroupId 
  , topicName :: TopicName
  , offsets :: IntMap Int64
  , consumerOpts :: ConsumerOptions
  }

data AutoCommit = AutoCommit | NoAutoCommit
data AutoHeartbeat = AutoHeartbeat | NoAutoHeartbaet

data ConsumerOptions = ConsumerOptions
  { autoCommit :: AutoCommit
  , autoHeartbeat :: AutoHeartbeat
  , maxFetchWaitTime :: Int
  , maxFetchBytes :: Int
  , fetchStart :: KafkaTimestamp
  }

defaultConsumerOptions :: ConsumerOptions
defaultConsumerOptions = ConsumerOptions
  { autoCommit = AutoCommit
  , autoHeartbeat = AutoHeartbeat
  , maxFetchWaitTime = 5000000
  , maxFetchBytes = 30000
  , fetchStart = Earliest
  }

newConsumer :: ByteString -> ByteString -> ConsumerOptions -> IO Consumer
newConsumer (fromBS -> groupId) (fromBS -> topicName) opts = do
  newKafka (peer opts) >>= \case
    Left err -> throw err
    Right kafka -> do
      mv <- newMVar kafka
      pure (Consumer mv groupId topicName opts)
  where
  fromBS = coerce . fromByteString

getRecordSet :: Consumer -> IO FetchResponse
getRecordSet consumer = do
  fetch (offsets consumer)
  getFetchResponse ...

commitOffsets :: Consumer -> IO (Either KafkaException ())
commitOffsets consumer = do
  offsetCommit (offsets consumer)
  getOffsetCommitResponse ...

app :: IO ()
app = do
  c <- newConsumer "groupid" "topic" defaultConsumerOptions
  go c
  where
  go c = forever $ do
    records <- getRecordSet c
    doStuffWith records
    commitOffsets c 

{-

-- Is known before we join a group
data ConsumerSettingStuff = ConsumerSettingStuff
  { top :: TopicName
  , lastRequestTime :: Time
  , leave :: TVar Bool
  , maxFetchBytes :: Int
  , groupName :: ByteArray
  , fetchStart :: KafkaTimestamp
  }

-- Gets assigned after we successfully join a group
data ConsumerStateStuff =
  { assignments :: [Int32]
  , partitionCount :: Int32
  , genId :: GenerationId
  , memberId :: MemberId
  , offsets :: IntMap Int64
  }

data ConsumerStuff = Consumer (TVar ConsumerSettingStuff) (TVar ConsumerStateStuff)

newConsumer :: ConsumerSettingStuff -> IO ConsumerStuff
fetchStuff :: Kafka -> ConsumerStuff -> IO FetchResponse
commitStuff :: Kafka -> ConsumerStuff -> IO OffsetCommitResponse
heartbeat :: Kafka -> ConsumerStuff -> IO HeartbeatResponse
rejoin :: Kafka -> ConsumerStuff -> IO (Either Error ())

app = do
  k <- newKafka
  c <- newConsumer
  resp <- fetchStuff k c
  print resp

-}

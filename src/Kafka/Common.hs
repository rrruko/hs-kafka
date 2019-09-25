{-# language
    BangPatterns
  , DataKinds
  , DerivingStrategies
  , GeneralizedNewtypeDeriving
  , GADTs
  , LambdaCase
  , OverloadedStrings
  , ScopedTypeVariables
  , StandaloneDeriving
  , ViewPatterns
  #-}

module Kafka.Common
  ( Kafka(..)
  , withKafka
  , Topic(..)
  , TopicName(..)
  , getTopicName
  , PartitionOffset(..)
  , TopicAssignment(..)
  , Interruptedness(..)
  , KafkaTimestamp(..)
  , AutoCreateTopic(..)
  , KafkaException(..)
  , GroupMember(..)
  , GroupName(..)
  , GenerationId(..)
  , KafkaProtocolError(..)
  , MemberAssignment(..)
  , FetchErrorMessage(..)
  , OffsetCommitErrorMessage(..)
  , fromErrorCode
  , clientId
  , clientIdLength
  , correlationId
  ) where

import Socket.Stream.IPv4

import qualified String.Ascii as S

newtype Kafka = Kafka { getKafka :: Connection }

instance Show Kafka where
  show _ = "<Kafka>"

data Topic = Topic
  {-# UNPACK #-} !TopicName -- Topic name
  {-# UNPACK #-} !Int -- Number of partitions
  {-# UNPACK #-} !(IORef Int) -- incrementing number

newtype TopicName = TopicName S.String
  deriving newtype (Eq, Ord, Show, IsString)

getTopicName :: Topic -> TopicName
getTopicName (Topic name _ _) = name

data PartitionOffset = PartitionOffset
  { partitionIndex :: !Int32
  , partitionOffset :: !Int64
  } deriving (Eq, Show)

data KafkaTimestamp
  = Latest
  | Earliest
  | At !Int64
  deriving (Show)

data AutoCreateTopic
  = Create
  | NeverCreate
  deriving (Show)

data KafkaException where
  KafkaSendException :: ()
    => SendException 'Uninterruptible
    -> KafkaException
  KafkaReceiveException :: ()
    => ReceiveException 'Interruptible
    -> KafkaException
  KafkaCloseException :: ()
    => CloseException
    -> KafkaException
  KafkaParseException :: ()
    => String
    -> KafkaException
  KafkaUnexpectedErrorCodeException :: ()
    => !Int16
    -> KafkaException
  KafkaConnectException :: ()
    => ConnectException ('Internet 'V4) 'Uninterruptible
    -> KafkaException
  KafkaException :: ()
    => String
    -> KafkaException
  KafkaOffsetCommitException :: ()
    => [OffsetCommitErrorMessage]
    -> KafkaException
--  KafkaProduceException :: ()
--    => !Int16
--    -> KafkaException
  KafkaFetchException :: ()
    => [FetchErrorMessage]
    -> KafkaException
  KafkaProtocolException :: ()
    => !KafkaProtocolError
    -> KafkaException

deriving stock instance Show KafkaException

data OffsetCommitErrorMessage = OffsetCommitErrorMessage
  { commitErrorTopic :: {-# UNPACK #-} !TopicName
  , commitErrorPartition :: {-# UNPACK #-} !Int32
  , commitErrorCode :: {-# UNPACK #-} !Int16
  } deriving Show

data FetchErrorMessage = FetchErrorMessage
  { fetchErrorTopic :: {-# UNPACK #-} !TopicName
  , fetchErrorPartition :: {-# UNPACK #-} !Int32
  , fetchErrorCode :: {-# UNPACK #-} !Int16
  } deriving Show

data GroupMember = GroupMember
  { groupName :: !GroupName
  , memberId :: !(Maybe ByteArray)
  }
  deriving (Eq, Show)

newtype GroupName = GroupName
  { getGroupName :: S.String
  } deriving (Eq, Show, IsString)

newtype GenerationId = GenerationId
  { getGenerationId :: Int32
  } deriving (Eq, Show)

data MemberAssignment = MemberAssignment
  { assignedMemberId :: {-# UNPACK #-} !ByteArray
  , assignedTopics :: [TopicAssignment]
  } deriving (Eq, Show)

data TopicAssignment = TopicAssignment
  { assignedTopicName :: !TopicName
  , assignedPartitions :: [Int32]
  } deriving (Eq, Show)

data Interruptedness = Interrupted | Uninterrupted
  deriving (Eq, Show)

withKafka :: ()
  => Peer
  -> (Kafka -> IO a)
  -> IO (Either KafkaException a)
withKafka peer f = do
  r <- withConnection
    peer
    (\e a -> case e of
      Left c -> pure (Left (KafkaCloseException c))
      Right () -> pure a
    )
    (\(Kafka -> conn) -> fmap Right (f conn)
    )
  case r of
    Left e -> pure (Left (KafkaConnectException e))
    Right x -> pure x

clientId :: S.String
clientId = "ruko"

clientIdLength :: Int
clientIdLength = S.length clientId

correlationId :: Int32
correlationId = 0xbeef

-- | Kafka Protocol Error Codes.
--
--   Not all errors are 'fatal', though fatality is something
--   that can be considered application-specific.
--
--   These are documented further <https://kafka.apache.org/protocol#protocol_error_codes here>.
data KafkaProtocolError
  = UnknownServerError
    -- ^ Value: (-1)
    --
    --   Retriable: False
    --
    --   Description: The server experienced an unexpected error when
    --   processing the request.
  | None
    -- ^ Value: (0)
    --
    --   Retriable: False
    --
    --   Description: No error.
  | OffsetOutOfRange
    -- ^ Value: (1)
    --
    --   Retriable: False
    --
    --   Description: The requested offset is not within the
    --   range of offsets maintained by the server.
  | CorruptMessage
    -- ^ Value: (2)
    --
    --   Retriable: True
    --
    --   Description: This message has failed its CRC checksum,
    --   exceeds the valid size, has a null key for a compacted
    --   topic, or is otherwise corrupt.
  | UnknownTopicOrPartition
    -- ^ Value: (3).
    --
    --   Retriable: True
    --
    --   Description: This server does not host this topic-partition.
  | InvalidFetchSize
    -- ^ Value: (4)
    --
    --   Retriable: False
    --
    --   Description: The requested fetch size is invalid.
  | LeaderNotAvailable
    -- ^ Value: (5)
    --
    --   Retriable: True
    --
    --   Description: There is no leader for this topic-partition
    --   as we are in the middle of a leadership election.
  | NotLeaderForPartition
    -- ^ Value: (6)
    --
    --   Retriable: True
    --
    --   Description: This server is not the leader for that
    --   topic-partition.
  | RequestTimedOut
    -- ^ Value: (7)
    --
    --   Retriable: True
    --
    --   Description: The request timed out.
  | BrokerNotAvailable
    -- ^ Value: (8)
    --
    --   Retriable: False
    --
    --   Description: The broker is not available.
  | ReplicaNotAvailable
    -- ^ Value: (9)
    --
    --   Retriable: False
    --
    --   Description: The replica is not available for the
    --   requested topic-partition.
  | MessageTooLarge
    -- ^ Value: (10)
    --
    --   Retriable: False
    --
    --   Description: The request included a message larger than
    --   the max message size the server will accept.
  | StaleControllerEpoch
    -- ^ Value: (11)
    --
    --   Retriable: False
    --
    --   Description: The controller moved to another broker.
  | OffsetMetadataTooLarge
    -- ^ Value: (12)
    --
    --   Retriable: False
    --
    --   Description: The metadata field of the offset request
    --   was too large.
  | NetworkException
    -- ^ Value: (13)
    --
    --   Retriable: True
    --
    --   Description: The server disconnected before a response
    --   was received.
  | CoordinatorLoadInProgress
    -- ^ Value: (14)
    --
    --   Retriable: True
    --
    --   Description: The coordinator is loading and hence
    --   can't process requests.
  | CoordinatorNotAvailable
    -- ^ Value: (15)
    --
    --   Retriable: True
    --
    --   Description: The coordinator is not available.
  | NotCoordinator
    -- ^ Value: (16)
    --
    --   Retriable: True
    --
    --   Description: This is not the correct coordinator.
  | InvalidTopicException
    -- ^ Value: (17)
    --
    --   Retriable: False
    --
    --   Description: The request attempted to perform an operation
    --   on an invalid topic.
  | RecordListTooLarge
    -- ^ Value: (18)
    --
    --   Retriable: False
    --
    --   Description: The request included message batch larger
    --   than the configured segment size on the server.
  | NotEnoughReplicas
    -- ^ Value: (19)
    --
    --   Retriable: True
    --
    --   Description: Messages are rejected since there are fewer
    --   in-sync replicas than required.
  | NotEnoughReplicasAfterAppend
    -- ^ Value: (20)
    --
    --   Retriable: True
    --
    --   Description: Messages are written to the log, but to
    --   fewer in-sync replicas than required.
  | InvalidRequiredAcks
    -- ^ Value: (21)
    --
    --   Retriable: False
    --
    --   Description: Produce request specified an invalid value
    --   for required acks.
  | IllegalGeneration
    -- ^ Value: (22)
    --
    --   Retriable: False
    --
    --   Description: Specified group generation id is not valid.
  | InconsistentGroupProtocol
    -- ^ Value: (23)
    --
    --   Retriable: False
    --
    --   Description: The group member's supported protocols are
    --   incompatible with those of existing members or first group
    --   member tried to join with empty protocol type or empty
    --   protocol list.
  | InvalidGroupId
    -- ^ Value: (24)
    --
    --   Retriable: False
    --
    --   Description: The configured groupId is invalid.
  | UnknownMemberId
    -- ^ Value: (25)
    --
    --   Retriable: False
    --
    --   Description: The coordinator is not aware of this member.
  | InvalidSessionTimeout
    -- ^ Value: (26)
    --
    --   Retriable: False
    --
    --   Description: The session timeout is not within the range
    --   allowed by the broker
    --   (as configured by group.min.session.timeout.ms and
    --   group.max.session.timeout.ms).
  | RebalanceInProgress
    -- ^ Value: (27)
    --
    --   Retriable: False
    --
    --   Description: The group is rebalancing, so a rejoin is needed.
  | InvalidCommitOffsetSize
    -- ^ Value: (28)
    --
    --   Retriable: False
    --
    --   Description: The committing offset data size is not valid.
  | TopicAuthorizationFailed
    -- ^ Value: (29)
    --
    --   Retriable: False
    --
    --   Description: Not authorized to access topic(s) (Topic
    --   authorization failed).
  | GroupAuthorizationFailed
    -- ^ Value: (30)
    --
    --   Retriable: False
    --
    --   Description: Not authorized to access group(s) (Group
    --   authorization failed).
  | ClusterAuthorizationFailed
    -- ^ Value: (31)
    --
    --   Retriable: False
    --
    --   Description: Cluster authorization failed.
  | InvalidTimestamp
    -- ^ Value: (32)
    --
    --   Retriable: False
    --
    --   Description: The timestamp of the message is out of acceptable
    --   range.
  | UnsupportedSaslMechanism
    -- ^ Value: (33)
    --
    --   Retriable: False
    --
    --   Description: The broker does not support the requested
    --   SASL mechanism.
  | IllegalSaslState
    -- ^ Value: (34)
    --
    --   Retriable: False
    --
    --   Description: Request is not valid given the current SASL state.
  | UnsupportedVersion
    -- ^ Value: (35)
    --
    --   Retriable: False
    --
    --   Description: The version of API is not supported.
  | TopicAlreadyExists
    -- ^ Value: (36)
    --
    --   Retriable: False
    --
    --   Description: Topic with this name already exists.
  | InvalidPartitions
    -- ^ Value: (37)
    --
    --   Retriable: False
    --
    --   Description: Number of partitions is below 1.
  | InvalidReplicationFactor
    -- ^ Value: (38)
    --
    --   Retriable: False
    --
    --   Description: Replication factor is below 1 or larger
    --   than the number of available brokers.
  | InvalidReplicaAssignment
    -- ^ Value: (39)
    --
    --   Retriable: False
    --
    --   Description: Replicate assignment is invalid.
  | InvalidConfig
    -- ^ Value: (40)
    --
    --   Retriable: False
    --
    --   Description: Server configuration is invalid.
  | NotController
    -- ^ Value: (41)
    --
    --   Retriable: True
    --
    --   Description: This is not the correct controller for
    --   this cluster.
  | InvalidRequest
    -- ^ Value: (42)
    --
    --   Retriable: False
    --
    --   Description: This most likely occurs because of a request
    --   being malformed by the client library or the message was
    --   sent to an incompatible broker. See the broker logs for
    --   more details.
  | UnsupportedForMessageFormat
    -- ^ Value: (43)
    --
    --   Retriable: False
    --
    --   Description: The message format version on the broker does
    --   not support the request.
  | PolicyViolation
    -- ^ Value: (44)
    --
    --   Retriable: False
    --
    --   Description: Request parameters do not satisfy the configured
    --   policy.
  | OutOfOrderSequenceNumber
    -- ^ Value: (45)
    --
    --   Retriable: False
    --
    --   Description: The broker received an out of order sequence
    --   number.
  | DuplicateSequenceNumber
    -- ^ Value: (46)
    --
    --   Retriable: False
    --
    --   Description: The broker received a duplicate sequence number.
  | InvalidProducerEpoch
    -- ^ Value: (47)
    --
    --   Retriable: False
    --
    --   Description: Producer attempted an operation with an old
    --   epoch. Either there is a newer producer with the same
    --   transactionalId, or the producer's transaction has been
    --   expired by the broker.
  | InvalidTxnState
    -- ^ Value: (48)
    --
    --   Retriable: False
    --
    --   Description: The producer attempted a transactional operation
    --   in an invalid state.
  | InvalidProducerIdMapping
    -- ^ Value: (49)
    --
    --   Retriable: False
    --
    --   Description: The producer attempted to use a producer id which
    --   is not currently assigned to its transactional id.
  | InvalidTransactionTimeout
    -- ^ Value: (50)
    --
    --   Retriable: False
    --
    --   Description: The transaction timeout is larger than the
    --   maximum value allowed by the broker (as configured by
    --   transaction.max.timeout.ms).
  | ConcurrentTransactions
    -- ^ Value: (51)
    --
    --   Retriable: False
    --
    --   Description: The producer attempted to update a transaction
    --   while another concurrent operation on the same transaction
    --   was ongoing.
  | TransactionCoordinatorFenced
    -- ^ Value: (52)
    --
    --   Retriable: False
    --
    --   Description: Indicates that the transaction coordinator
    --   sending a WriteTxnMarker is no longer the current coordinator
    --   for a given producer.
  | TransactionalIdAuthorizationFailed
    -- ^ Value: (53)
    --
    --   Retriable: False
    --
    --   Description: Transaction Id authorization failed.
  | SecurityDisabled
    -- ^ Value: (54)
    --
    --   Retriable: False
    --
    --   Description: Security features are disabled.
  | OperationNotAttempted
    -- ^ Value: (55)
    --
    --   Retriable: False
    --
    --   Description: The broker did not attempt to execute this
    --   operation. This may happen for batched RPCs where some
    --   operations in the batch failed, causing the broker to
    --   respond without trying the rest.
  | KafkaStorageError
    -- ^ Value: (56)
    --
    --   Retriable: True
    --
    --   Description: Disk error when trying to access log file
    --   on the disk.
  | LogDirNotFound
    -- ^ Value: (57)
    --
    --   Retriable: False
    --
    --   Description: The user-specified log directory is not found
    --   in the broker config.
  | SaslAuthenticationFailed
    -- ^ Value: (58)
    --
    --   Retriable: False
    --
    --   Description: SASL Authentication failed.
  | UnknownProducerId
    -- ^ Value: (59)
    --
    --   Retriable: False
    --
    --   Description: This exception is raised by the broker if it
    --   could not locate the producer metadata associated with the
    --   producerId in question. This could happen if, for instance,
    --   the producer's records were deleted because their retention
    --   time had elapsed. Once the last records of the producerId
    --   are removed, the producer's metadata is removed from the
    --   broker, and future appends by the producer will return
    --   this exception.
  | ReassignmentInProgress
    -- ^ Value: (60)
    --
    --   Retriable: False
    --
    --   Description: A partition reassignment is in progress.
  | DelegationTokenAuthDisabled
    -- ^ Value: (61)
    --
    --   Retriable: False
    --
    --   Description: Delegation Token feature is not enabled.
  | DelegationTokenNotFound
    -- ^ Value: (62)
    --
    --   Retriable: False
    --
    --   Description: Delegation Token is not found on server.
  | DelegationTokenOwnerMismatch
    -- ^ Value: (63)
    --
    --   Retriable: False
    --
    --   Description: Specified Principal is not valid Owner/Renewer.
  | DelegationTokenRequestNotAllowed
    -- ^ Value: (64)
    --
    --   Retriable: False
    --
    --   Description: Delegation Token requests are not allowed on
    --   PLAINTEXT/1-way SSL channels and on delegation token
    --   authenticated channels.
  | DelegationTokenAuthorizationFailed
    -- ^ Value: (65)
    --
    --   Retriable: False
    --
    --   Description: Delegation Token authorization failed.
  | DelegationTokenExpired
    -- ^ Value: (66)
    --
    --   Retriable: False
    --
    --   Description: Delegation Token is expired.
  | InvalidPrincipalType
    -- ^ Value: (67)
    --
    --   Retriable: False
    --
    --   Description: Supplied principalType is not supported.
  | NonEmptyGroup
    -- ^ Value: (68)
    --
    --   Retriable: False
    --
    --   Description: The group is not empty.
  | GroupIdNotFound
    -- ^ Value: (69)
    --
    --   Retriable: False
    --
    --   Description: The group id does not exist.
  | FetchSessionIdNotFound
    -- ^ Value: (70)
    --
    --   Retriable: True
    --
    --   Description: The fetch session ID was not found.
  | InvalidFetchSessionEpoch
    -- ^ Value: (71)
    --
    --   Retriable: True
    --
    --   Description: The fetch session epoch is invalid.
  | ListenerNotFound
    -- ^ Value: (72)
    --
    --   Retriable: True
    --
    --   Description: There is no listener on the leader
    --   broker that matches the listener on which metadata request
    --   was processed.
  | TopicDeletionDisabled
    -- ^ Value: (73)
    --
    --   Retriable: False
    --
    --   Description: Topic deletion is disabled.
  | FencedLeaderEpoch
    -- ^ Value: (74)
    --
    --   Retriable: True
    --
    --   Description: The leader epoch in the request is older
    --   than the epoch on the broker.
  | UnknownLeaderEpoch
    -- ^ Value: (75)
    --
    --   Retriable: True
    --
    --   Description: The leader epoch in the request is newer
    --   than the epoch on the broker.
  | UnsupportedCompressionType
    -- ^ Value: (76)
    --
    --   Retriable: False
    --
    --   Description: The requesting client does not support the
    --   compression type of given partition.
  | StaleBrokerEpoch
    -- ^ Value: (77)
    --
    --   Retriable: False
    --
    --   Description: Broker epoch has changed.
  | OffsetNotAvailable
    -- ^ Value: (78)
    --
    --   Retriable: True
    --
    --   Description: The leader high watermark has not caught up
    --   from a recent leader election so the offsets cannot be
    --   guaranteed to be monotonically increasing.
  | MemberIdRequired
    -- ^ Value: (79)
    --
    --   Retriable: False
    --
    --   Description: The group member needs to have a valid member id
    --   before actually entering a consumer group.
  | PreferredLeaderNotAvailable
    -- ^ Value: (80)
    --
    --   Retriable: True
    --
    --   Description: The preferred leader was not available.
  | GroupMaxSizeReached
    -- ^ Value: (81)
    --
    --   Retriable: False
    --
    --   Description: The consumer group has reached its max
    --   size (already has the configured maximum number of members).
  | FencedInstanceId
    -- ^ Value: (82)
    --
    --   Retriable: False
    --
    --   Description: The broker rejected this static consumer
    --   since another consumer with the same group.instance.id has
    --   registered with a different member.id.
  deriving stock (Eq, Show)

fromErrorCode :: Int16 -> Maybe KafkaProtocolError
fromErrorCode = \case
  (-1) -> Just UnknownServerError
  0    -> Just None
  1    -> Just OffsetOutOfRange
  2    -> Just CorruptMessage
  3    -> Just UnknownTopicOrPartition
  4    -> Just InvalidFetchSize
  5    -> Just LeaderNotAvailable
  6    -> Just NotLeaderForPartition
  7    -> Just RequestTimedOut
  8    -> Just BrokerNotAvailable
  9    -> Just ReplicaNotAvailable
  10   -> Just MessageTooLarge
  11   -> Just StaleControllerEpoch
  12   -> Just OffsetMetadataTooLarge
  13   -> Just NetworkException
  14   -> Just CoordinatorLoadInProgress
  15   -> Just CoordinatorNotAvailable
  16   -> Just NotCoordinator
  17   -> Just InvalidTopicException
  18   -> Just RecordListTooLarge
  19   -> Just NotEnoughReplicas
  20   -> Just NotEnoughReplicasAfterAppend
  21   -> Just InvalidRequiredAcks
  22   -> Just IllegalGeneration
  23   -> Just InconsistentGroupProtocol
  24   -> Just InvalidGroupId
  25   -> Just UnknownMemberId
  26   -> Just InvalidSessionTimeout
  27   -> Just RebalanceInProgress
  28   -> Just InvalidCommitOffsetSize
  29   -> Just TopicAuthorizationFailed
  30   -> Just GroupAuthorizationFailed
  31   -> Just ClusterAuthorizationFailed
  32   -> Just InvalidTimestamp
  33   -> Just UnsupportedSaslMechanism
  34   -> Just IllegalSaslState
  35   -> Just UnsupportedVersion
  36   -> Just TopicAlreadyExists
  37   -> Just InvalidPartitions
  38   -> Just InvalidReplicationFactor
  39   -> Just InvalidReplicaAssignment
  40   -> Just InvalidConfig
  41   -> Just NotController
  42   -> Just InvalidRequest
  43   -> Just UnsupportedForMessageFormat
  44   -> Just PolicyViolation
  45   -> Just OutOfOrderSequenceNumber
  46   -> Just DuplicateSequenceNumber
  47   -> Just InvalidProducerEpoch
  48   -> Just InvalidTxnState
  49   -> Just InvalidProducerIdMapping
  50   -> Just InvalidTransactionTimeout
  51   -> Just ConcurrentTransactions
  52   -> Just TransactionCoordinatorFenced
  53   -> Just TransactionalIdAuthorizationFailed
  54   -> Just SecurityDisabled
  55   -> Just OperationNotAttempted
  56   -> Just KafkaStorageError
  57   -> Just LogDirNotFound
  58   -> Just SaslAuthenticationFailed
  59   -> Just UnknownProducerId
  60   -> Just ReassignmentInProgress
  61   -> Just DelegationTokenAuthDisabled
  62   -> Just DelegationTokenNotFound
  63   -> Just DelegationTokenOwnerMismatch
  64   -> Just DelegationTokenRequestNotAllowed
  65   -> Just DelegationTokenAuthorizationFailed
  66   -> Just DelegationTokenExpired
  67   -> Just InvalidPrincipalType
  68   -> Just NonEmptyGroup
  69   -> Just GroupIdNotFound
  70   -> Just FetchSessionIdNotFound
  71   -> Just InvalidFetchSessionEpoch
  72   -> Just ListenerNotFound
  73   -> Just TopicDeletionDisabled
  74   -> Just FencedLeaderEpoch
  75   -> Just UnknownLeaderEpoch
  76   -> Just UnsupportedCompressionType
  77   -> Just StaleBrokerEpoch
  78   -> Just OffsetNotAvailable
  79   -> Just MemberIdRequired
  80   -> Just PreferredLeaderNotAvailable
  81   -> Just GroupMaxSizeReached
  82   -> Just FencedInstanceId
  _    -> Nothing

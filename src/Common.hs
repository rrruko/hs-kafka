{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module Common where

import Data.ByteString (ByteString)
import Data.Bytes.Types
import Data.Int
import Data.IORef
import Data.Primitive
import Data.Primitive.Unlifted.Array
import Data.Word
import Socket.Stream.IPv4

import qualified Data.ByteString as BS

newtype Kafka = Kafka { getKafka :: Connection }

data Topic = Topic
  ByteArray -- Topic name
  Int -- Number of partitions
  (IORef Int) -- incrementing number

data KafkaException = KafkaException String
  deriving Show

newKafka :: Peer -> IO (Either (ConnectException ('Internet 'V4) 'Uninterruptible) Kafka)
newKafka = fmap (fmap Kafka) . connect

toBE16 :: Int16 -> Int16
toBE16 = fromIntegral . byteSwap16 . fromIntegral

toBE32 :: Int32 -> Int32
toBE32 = fromIntegral . byteSwap32 . fromIntegral

toBEW32 :: Word32 -> Word32
toBEW32 = byteSwap32

toBE64 :: Int64 -> Int64
toBE64 = fromIntegral . byteSwap64 . fromIntegral

size8 :: ByteArray -> Int8
size8 = fromIntegral . sizeofByteArray

size16 :: ByteArray -> Int16
size16 = fromIntegral . sizeofByteArray

size32 :: ByteArray -> Int32
size32 = fromIntegral . sizeofByteArray

getArray :: MutableBytes s -> MutableByteArray s
getArray (MutableBytes a _ _) = a

toByteString :: ByteArray -> ByteString
toByteString = BS.pack . foldrByteArray (:) []

fromByteString :: ByteString -> ByteArray
fromByteString = byteArrayFromList . BS.unpack

toKafkaException :: Show a => a -> KafkaException
toKafkaException = KafkaException . show

foldByteArrays :: UnliftedArray ByteArray -> ByteArray
foldByteArrays = foldrUnliftedArray (<>) (byteArrayFromList ([]::[Char]))

clientId :: ByteString
clientId = "ruko"

clientIdLength :: Int
clientIdLength = BS.length clientId

correlationId :: Int32
correlationId = 0xbeef

magic :: Int8
magic = 2

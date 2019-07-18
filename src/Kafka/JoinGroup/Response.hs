{-# LANGUAGE OverloadedStrings #-}

module Kafka.JoinGroup.Response
  ( JoinGroupResponse(..)
  , getJoinGroupResponse
  , parseJoinGroupResponse
  ) where

import Data.Attoparsec.ByteString (Parser, (<?>))
import Data.ByteString (ByteString)
import Data.Int
import GHC.Conc

import Kafka.Combinator
import Kafka.Common
import Kafka.Response

data JoinGroupResponse = JoinGroupResponse
  { throttleTimeMs :: Int32
  , errorCode :: Int16
  , generationId :: Int32
  , groupProtocol :: ByteString
  , leaderId :: ByteString
  , memberId :: ByteString
  , members :: [Member]
  } deriving (Eq, Show)

data Member = Member
  { groupMemberId :: ByteString
  , groupMemberMetadata :: ByteString
  } deriving (Eq, Show)

parseJoinGroupResponse :: Parser JoinGroupResponse
parseJoinGroupResponse = do
  _correlationId <- int32 <?> "correlation id"
  JoinGroupResponse
    <$> (int32 <?> "throttle time")
    <*> (int16 <?> "error code")
    <*> (int32 <?> "generation id")
    <*> (byteString <?> "group protocol")
    <*> (byteString <?> "leader id")
    <*> (byteString <?> "member id")
    <*> (array parseMember <?> "members")

parseMember :: Parser Member
parseMember = Member
  <$> (byteString <?> "member id")
  <*> (sizedBytes <?> "member metadata")

getJoinGroupResponse ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException (Either String JoinGroupResponse))
getJoinGroupResponse = fromKafkaResponse parseJoinGroupResponse

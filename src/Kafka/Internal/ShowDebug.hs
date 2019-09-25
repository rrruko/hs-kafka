module Kafka.Internal.ShowDebug
  (ShowDebug(..)
  ) where

import Data.List (intercalate)
import Kafka.Common

import qualified String.Ascii as S

class ShowDebug a where
  showDebug :: a -> String

instance ShowDebug Int8 where
  showDebug = show

instance ShowDebug Int16 where
  showDebug = show

instance ShowDebug Int32 where
  showDebug = show

instance ShowDebug Int64 where
  showDebug = show

instance ShowDebug Int where
  showDebug = show

instance ShowDebug a => ShowDebug [a] where
  showDebug xs = "[" <> intercalate ", " (fmap showDebug xs) <> "]"

instance ShowDebug a => ShowDebug (Maybe a) where
  showDebug Nothing = "Nothing"
  showDebug (Just x) = "Just " <> showDebug x

instance ShowDebug ByteArray where
  showDebug = show . S.fromByteArray

instance ShowDebug S.String where
  showDebug b = S.asByteArray b show

instance ShowDebug GroupName where
  showDebug (GroupName gid) = "GroupName(" <> showDebug gid <> ")"

instance ShowDebug Topic where
  showDebug (Topic topicName parts _) =
    "Topic (" <> showDebug topicName <> ", " <> show parts <> ")"

instance ShowDebug TopicName where
  showDebug (TopicName t) = showDebug t

instance ShowDebug GroupMember where
  showDebug (GroupMember gid mid) =
    "GroupMember (" <> showDebug gid <> ", " <> showDebug mid <> ")"

instance ShowDebug MemberAssignment where
  showDebug (MemberAssignment mid assignments) =
    "MemberAssignment (" <> showDebug mid <> ", " <> showDebug assignments <> ")"

instance ShowDebug TopicAssignment where
  showDebug (TopicAssignment topicName partitions) =
    "TopicAssignment (" <> showDebug topicName <> ", " <> show partitions <> ")"

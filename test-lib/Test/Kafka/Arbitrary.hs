{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}
module Test.Kafka.Arbitrary
    ( Payload(..)
    , NonEmptyPayloadList(..)
    ) where

import Control.Applicative
import Test.QuickCheck

import qualified Data.ByteString       as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Time             as T

import qualified Kafka.Internal.Types as I

instance Arbitrary I.Error where
    arbitrary = elements [
        I.UnknownError
      , I.OffsetOutOfRangeError
      , I.InvalidMessageError
      , I.WrongPartitionError
      , I.InvalidFetchSizeError
      ]

instance Arbitrary I.Compression where
    arbitrary = elements [
        I.NoCompression
      , I.SnappyCompression
      , I.GzipCompression
      ]

instance Arbitrary I.OffsetsTime where
    arbitrary = oneof [
        elements [I.OffsetsLatest, I.OffsetsEarliest]
      , I.OffsetsBefore <$> someTime
      ]
      where
        someTime = T.UTCTime <$> someDay <*> someDayTime
        someDay =
            -- Note: We only support times after Unix epoch. Any dates before
            -- 1970 cannot be represented in Kafka 0.7.
            T.fromGregorian <$> choose (1970, 2400)
                            <*> choose (1, 12)
                            <*> choose (0, 31)
        someDayTime = T.secondsToDiffTime <$> choose (0, 86400)

instance Arbitrary I.RequestType where
    arbitrary = elements [
        I.ProduceRequestType
      , I.FetchRequestType
      , I.MultiFetchRequestType
      , I.MultiProduceRequestType
      , I.OffsetsRequestType
      ]

instance Arbitrary I.Topic where
    arbitrary = I.Topic . B8.pack <$> safeString
      where
        safeString = listOf1 $ elements ['a'..'z']

deriving instance Arbitrary I.Offset
deriving instance Arbitrary I.Partition
deriving instance Arbitrary I.Size
deriving instance Arbitrary I.Count

instance Arbitrary I.Message where
    arbitrary =
        I.Message <$> arbitrary
                  <*> (getPayload <$> arbitrary)

instance Arbitrary I.MessageSet where
    arbitrary = I.MessageSet . map getPayload <$> arbitrary

newtype Payload = Payload { getPayload :: B.ByteString }
    deriving (Show)

instance Arbitrary Payload where
    arbitrary = Payload . B.pack <$> arbitrary

newtype NonEmptyPayloadList = NonEmptyPayloadList
    { getPayloads :: [B.ByteString]
    } deriving (Show)

instance Arbitrary NonEmptyPayloadList where
    arbitrary = NonEmptyPayloadList . map getPayload <$> listOf1 arbitrary

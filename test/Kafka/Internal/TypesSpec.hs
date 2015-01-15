{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}
module Kafka.Internal.TypesSpec where

import Control.Applicative
import Test.Hspec
import Test.QuickCheck

import qualified Data.ByteString as B
import qualified Data.Serialize  as C
import qualified Data.Time       as T

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
    arbitrary = I.Topic . B.pack . getNonEmpty <$> arbitrary

deriving instance Arbitrary I.Offset
deriving instance Arbitrary I.Partition
deriving instance Arbitrary I.Size
deriving instance Arbitrary I.Count

instance Arbitrary I.Message where
    arbitrary =
        I.Message <$> arbitrary
                  <*> (B.pack . getNonEmpty <$> arbitrary)

instance Arbitrary I.MessageSet where
    arbitrary = I.MessageSet <$> listOf1 somePayload
      where
        somePayload = B.pack . getNonEmpty <$> arbitrary

spec :: Spec
spec = do
    describe "Error" $
        it "serializes and deserializes" $
            property (checkSerialization :: Maybe I.Error -> Expectation)

    describe "Compression" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.Compression -> Expectation)

    describe "OffsetsTime" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.OffsetsTime -> Expectation)

    describe "RequestType" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.RequestType -> Expectation)

    describe "Topic" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.Topic -> Expectation)

    describe "Offset" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.Offset -> Expectation)

    describe "Partition" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.Partition -> Expectation)

    describe "Size" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.Size -> Expectation)

    describe "Count" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.Count -> Expectation)

    describe "Message" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.Message -> Expectation)

    describe "MessageSet" $
        it "serializes and deserializes" $
            property (checkSerialization :: I.MessageSet -> Expectation)

-- | Check that the given serializable item serializes correctly in both
-- directions.
checkSerialization :: (C.Serialize a, Show a, Eq a) => a -> Expectation
checkSerialization a = C.decode (C.encode a) `shouldBe` Right a

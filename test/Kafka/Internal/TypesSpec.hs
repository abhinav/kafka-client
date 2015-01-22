module Kafka.Internal.TypesSpec where

import Test.Hspec
import Test.QuickCheck

import qualified Data.Serialize as C

import Test.Kafka.Arbitrary ()

import qualified Kafka.Internal.Types as I

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

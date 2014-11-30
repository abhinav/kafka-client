module Kafka.V07.Internal.Request (
      Produce(..)
    , putProduceRequest
    , putMultiProduceRequest

    , Fetch(..)
    , putFetchRequest
    , putMultiFetchRequest

    , Offsets(..)
    , putOffsetsRequest
    ) where

import Control.Applicative
import Data.Foldable       (Foldable)
import Data.Word           (Word32)

import qualified Data.ByteString as B
import qualified Data.Foldable   as Fold
import qualified Data.Serialize  as C

import Kafka.V07.Internal.Types

putWithCountPrefix :: Foldable f => f a -> C.Putter a -> C.Put
putWithCountPrefix xs put =
    let (count, p) = Fold.foldr' go (0, pure ()) xs
    in C.putWord32be count *> p
  where
    go a (c, p) = (c + 1, put a *> p)

putWithLengthPrefix :: C.Put -> C.Put
putWithLengthPrefix p = do
    C.putWord32be $ fromIntegral (B.length bs)
    C.putByteString bs
  where
    bs = C.runPut p

encodeRequest :: RequestType -> C.Put -> C.Put
encodeRequest reqType p = putWithLengthPrefix $ C.put reqType >> p

data Produce =
    Produce {-# UNPACK #-} !Topic
            {-# UNPACK #-} !Partition
                           [Message]
  deriving (Show, Read, Eq)

encodeProduce :: Produce -> C.Put
encodeProduce (Produce topic partition messages) = do
    C.put topic
    C.put partition
    putWithCountPrefix messages C.put

putProduceRequest :: Produce -> C.Put
putProduceRequest =
    encodeRequest ProduceRequestType . encodeProduce

putMultiProduceRequest :: [Produce] -> C.Put
putMultiProduceRequest reqs =
    encodeRequest MultiProduceRequestType $
        putWithCountPrefix reqs encodeProduce

data Fetch =
    Fetch {-# UNPACK #-} !Topic
          {-# UNPACK #-} !Partition
          {-# UNPACK #-} !Offset
          {-# UNPACK #-} !Word32
  deriving (Show, Read, Eq)

encodeFetch :: Fetch -> C.Put
encodeFetch (Fetch topic partition offset maxSize) = do
    C.put topic
    C.put partition
    C.put offset
    C.putWord32be maxSize

putFetchRequest :: Fetch -> C.Put
putFetchRequest = encodeRequest FetchRequestType . encodeFetch

putMultiFetchRequest :: [Fetch] -> C.Put
putMultiFetchRequest reqs =
    encodeRequest MultiFetchRequestType $
        putWithCountPrefix reqs encodeFetch

data Offsets =
    Offsets {-# UNPACK #-} !Topic
            {-# UNPACK #-} !Partition
                           !OffsetsTime
            {-# UNPACK #-} !Word32
  deriving (Show, Read, Eq)

putOffsetsRequest :: Offsets -> C.Put
putOffsetsRequest (Offsets topic partition time maxNumber) =
    encodeRequest OffsetsRequestType $ do
        C.put topic
        C.put partition
        C.put time
        C.putWord32be maxNumber


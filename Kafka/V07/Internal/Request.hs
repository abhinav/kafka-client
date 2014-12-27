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
import Data.ByteString     (ByteString)
import Data.Foldable       (Foldable)

import qualified Data.ByteString as B
import qualified Data.Foldable   as Fold
import qualified Data.Serialize  as C

import Kafka.V07.Internal.Types

putWithCountPrefix :: Foldable f => f a -> C.Putter a -> C.Put
putWithCountPrefix xs put =
    let (count, p) = Fold.foldr' go (0, pure ()) xs
    in C.putWord16be count *> p
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

-- | A request to send messages down a Kafka topic-partition pair.
data Produce = Produce {
    produceTopic     :: {-# UNPACK #-} !Topic
  -- ^ Kafka topic to which the messages will be sent.
  , producePartition :: {-# UNPACK #-} !Partition
  -- ^ Partition of the topic.
  , produceMessages  ::                [ByteString]
  -- ^ List of message payloads.
  } deriving (Show, Read, Eq)

encodeProduce :: Produce -> C.Put
encodeProduce (Produce topic partition messages) = do
    C.put topic
    C.put partition
    putWithLengthPrefix (C.put (MessageSet messages))

putProduceRequest :: Produce -> C.Put
putProduceRequest =
    encodeRequest ProduceRequestType . encodeProduce

putMultiProduceRequest :: [Produce] -> C.Put
putMultiProduceRequest reqs =
    encodeRequest MultiProduceRequestType $
        putWithCountPrefix reqs encodeProduce

-- | A request to fetch messages from a particular Kafka topic-partition pair.
data Fetch = Fetch {
    fetchTopic     :: {-# UNPACK #-} !Topic
  -- ^ Kafka topic from which messages will be fetched.
  , fetchPartition :: {-# UNPACK #-} !Partition
  -- ^ Partition of the topic.
  , fetchOffset    :: {-# UNPACK #-} !Offset
  -- ^ Offset at which the fetch will start.
  , fetchSize      :: {-# UNPACK #-} !Size
  -- ^ Maximum size of the returned messages.
  --
  -- Note, this is /not/ the number of messages. This is the maximum
  -- combined size of the returned messages.
  } deriving (Show, Read, Eq)

encodeFetch :: Fetch -> C.Put
encodeFetch (Fetch topic partition offset maxSize) = do
    C.put topic
    C.put partition
    C.put offset
    C.put maxSize

putFetchRequest :: Fetch -> C.Put
putFetchRequest = encodeRequest FetchRequestType . encodeFetch

putMultiFetchRequest :: [Fetch] -> C.Put
putMultiFetchRequest reqs =
    encodeRequest MultiFetchRequestType $
        putWithCountPrefix reqs encodeFetch

-- | A request to retrieve offset information from Kafka.
data Offsets = Offsets {
    offsetsTopic     :: {-# UNPACK #-} !Topic
  -- ^ Kafka topic from which offsets will be retrieved.
  , offsetsPartition :: {-# UNPACK #-} !Partition
  -- ^ Partition of the topic.
  , offsetsTime      ::                !OffsetsTime
  -- ^ Time around which offsets will be retrieved.
  , offsetsCount     :: {-# UNPACK #-} !Count
  -- ^ Maximum number of offsets that will be retrieved.
  } deriving (Show, Read, Eq)

putOffsetsRequest :: Offsets -> C.Put
putOffsetsRequest (Offsets topic partition time maxNumber) =
    encodeRequest OffsetsRequestType $ do
        C.put topic
        C.put partition
        C.put time
        C.put maxNumber


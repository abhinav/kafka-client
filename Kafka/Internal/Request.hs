module Kafka.Internal.Request (
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

import Kafka.Internal.Types

-- | 'Put's the items in the given Foldable in-order and prepends the result
-- with a 16-bit integer containing the total number of items that were put.
putWithCountPrefix :: Foldable f => f a -> C.Putter a -> C.Put
putWithCountPrefix xs put =
    let (count, p) = Fold.foldr' go (0, pure ()) xs
    in C.putWord16be count *> p
  where
    go a (c, p) = (c + 1, put a *> p)

-- | Prepends the given Put with a 32-bit integer containing the total size of
-- the given Put.
putWithLengthPrefix :: C.Put -> C.Put
putWithLengthPrefix p = do
    C.putWord32be $ fromIntegral (B.length bs)
    C.putByteString bs
  where
    bs = C.runPut p

-- | Encodes a request with the given 'RequestType'.
--
-- More specifically, this prepends the given put with the request type and
-- the size of the request.
encodeRequest :: RequestType -> C.Put -> C.Put
encodeRequest reqType p = putWithLengthPrefix $ C.put reqType >> p

-- | A request to send messages down a Kafka topic-partition pair.
--
-- Produce requests do not have a corresponding response. There is no way of
-- knowing in Kafka 0.7 if a message was successfully @Produce@d.
data Produce = Produce {
    produceTopic     :: {-# UNPACK #-} !Topic
  -- ^ Kafka topic to which the messages will be sent.
  , producePartition :: {-# UNPACK #-} !Partition
  -- ^ Partition of the topic.
  , produceMessages  ::                [ByteString]
  -- ^ List of message payloads.
  --
  -- For those concerned with low-leveld details: These messages will be
  -- compressed using <https://code.google.com/p/snappy/ Snappy> compression.
  } deriving (Show, Read, Eq)

encodeProduce :: Produce -> C.Put
encodeProduce (Produce topic partition messages) = do
    C.put topic
    C.put partition
    putWithLengthPrefix (C.put (MessageSet messages))

-- | @Put@s the given single @Produce@ request.
putProduceRequest :: Produce -> C.Put
putProduceRequest =
    encodeRequest ProduceRequestType . encodeProduce

-- | @Put@s the given @MultiProduce@ request.
putMultiProduceRequest :: [Produce] -> C.Put
putMultiProduceRequest reqs =
    encodeRequest MultiProduceRequestType $
        putWithCountPrefix reqs encodeProduce

-- | A request to fetch messages from a particular Kafka topic-partition pair.
--
-- 'Kafka.FetchResponse' contains responses for this kind of request.
data Fetch = Fetch {
    fetchTopic     :: {-# UNPACK #-} !Topic
  -- ^ Kafka topic from which messages will be fetched.
  , fetchPartition :: {-# UNPACK #-} !Partition
  -- ^ Partition of the topic.
  , fetchOffset    :: {-# UNPACK #-} !Offset
  -- ^ Offset at which the fetch will start.
  --
  -- Kafka offloads the responsiblity of knowing this to the client. That
  -- means that if an offset is specified here that is not a real message
  -- start, Kafka will spit out garbage.
  --
  -- Use 'Kafka.offsets' to find valid offsets.
  , fetchSize      :: {-# UNPACK #-} !Size
  -- ^ Maximum size of the returned messages.
  --
  -- Note, this is /not/ the number of messages. This is the maximum
  -- combined size of the returned /compressed/ messages.
  } deriving (Show, Read, Eq)

encodeFetch :: Fetch -> C.Put
encodeFetch (Fetch topic partition offset maxSize) = do
    C.put topic
    C.put partition
    C.put offset
    C.put maxSize

-- | @Put@s the given single @Fetch@ request.
putFetchRequest :: Fetch -> C.Put
putFetchRequest = encodeRequest FetchRequestType . encodeFetch

-- | @Put@s the given @MultiFetch@ request.
putMultiFetchRequest :: [Fetch] -> C.Put
putMultiFetchRequest reqs =
    encodeRequest MultiFetchRequestType $
        putWithCountPrefix reqs encodeFetch

-- | A request to retrieve offset information from Kafka.
--
-- The response for this kind of request is a list of 'Offset's.
data Offsets = Offsets {
    offsetsTopic     :: {-# UNPACK #-} !Topic
  -- ^ Kafka topic from which offsets will be retrieved.
  , offsetsPartition :: {-# UNPACK #-} !Partition
  -- ^ Partition of the topic.
  , offsetsTime      ::                !OffsetsTime
  -- ^ Time around which offsets will be retrieved.
  --
  -- If you provide a time for this, keep in mind that the response will not
  -- contain the precise offset that occurred around that time. It will return
  -- up to @offsetsCount@ offsets in descending, each being the first offset
  -- of every segment file for the specified partition with a modified time
  -- less than the specified time, and possibly a "high water mark" for the
  -- last segment of the partition (if it was modified before the specified
  -- time) which specifies the offset at which the next message to that
  -- partition will be written.
  , offsetsCount     :: {-# UNPACK #-} !Count
  -- ^ Maximum number of offsets that will be retrieved.
  } deriving (Show, Read, Eq)

-- | @Put@s the given @Offsets@ request.
putOffsetsRequest :: Offsets -> C.Put
putOffsetsRequest (Offsets topic partition time maxNumber) =
    encodeRequest OffsetsRequestType $ do
        C.put topic
        C.put partition
        C.put time
        C.put maxNumber


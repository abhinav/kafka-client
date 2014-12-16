{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverlappingInstances #-}
module Kafka.V07.Internal.Types
    ( Error(..)
    , Compression(..)
    , OffsetsTime(..)
    , RequestType(..)

    , Topic(..)
    , Offset(..)
    , Partition(..)

    , Message(..)
    ) where

import Control.Applicative
import Data.ByteString       (ByteString)
import Data.Digest.CRC32     (crc32)
import Data.Time             (UTCTime)
import Data.Time.Clock.POSIX
import Data.Word

import qualified Data.ByteString as B
import qualified Data.Serialize  as C

-- | Different errors returned by Kafka.
data Error
    = UnknownError          -- -1
    | OffsetOutOfRangeError --  1
    | InvalidMessageError   --  2
    | WrongPartitionError   --  3
    | InvalidFetchSizeError --  4
  deriving (Show, Read, Eq, Ord)

instance C.Serialize (Maybe Error) where
    -- A note about extensions:
    -- We need FlexibleInstances to allow GHC to let us declare an instance
    -- for the type @Maybe Error@. We also need OverlappingInstances to tell
    -- it to pick this (more specific) instance of Maybe Error over the
    -- generic one provided by cereal.
    get = do
        code <- C.getWord16be
        case code of
            0  -> return Nothing
            1  -> return $ Just OffsetOutOfRangeError
            2  -> return $ Just InvalidMessageError
            3  -> return $ Just WrongPartitionError
            4  -> return $ Just InvalidFetchSizeError
            -1 -> return $ Just UnknownError
            _  ->   fail $ "Unknown error code: " ++ show code

    put                     Nothing  = C.putWord16be 0
    put (Just OffsetOutOfRangeError) = C.putWord16be 1
    put (Just   InvalidMessageError) = C.putWord16be 2
    put (Just   WrongPartitionError) = C.putWord16be 3
    put (Just InvalidFetchSizeError) = C.putWord16be 4
    put (Just          UnknownError) = C.putWord16be (-1)

data Compression
    = NoCompression
    | GzipCompression
    | SnappyCompression
  deriving (Show, Read, Eq, Ord)

instance C.Serialize Compression where
    put NoCompression     = C.putWord8 0
    put GzipCompression   = C.putWord8 1
    put SnappyCompression = C.putWord8 2

    get = C.getWord8 >>= \i -> case i of
        0 -> return NoCompression
        1 -> return GzipCompression
        2 -> return SnappyCompression
        _ -> fail $ "Invalid compression code: " ++ show i

data OffsetsTime
    = OffsetsLatest
    | OffsetsEarliest
    | OffsetsBefore !UTCTime
  deriving (Show, Read, Eq, Ord)

instance C.Serialize OffsetsTime where
    put OffsetsLatest = C.putWord64be (-1)
    put OffsetsEarliest = C.putWord64be (-2)
    put (OffsetsBefore t) =
        C.putWord64be . round . (* 1000) . utcTimeToPOSIXSeconds $ t

    -- Kafka doesn't care about anything beyond milliseconds while UTCTime can
    -- be more precise. We will lose some amount of precision if the same
    -- UTCTime is serialized and then deserialized.
    get = C.getWord64be >>= \i -> case i of
        -1 -> return   OffsetsLatest
        -2 -> return   OffsetsEarliest
        t  -> return . OffsetsBefore .
               posixSecondsToUTCTime . (/ 1000) . realToFrac $ t

data RequestType
    = ProduceRequestType        -- 0
    | FetchRequestType          -- 1
    | MultiFetchRequestType     -- 2
    | MultiProduceRequestType   -- 3
    | OffsetsRequestType        -- 4
  deriving (Show, Read, Eq, Ord)

instance C.Serialize RequestType where
    put ProduceRequestType      = C.putWord16be 0
    put FetchRequestType        = C.putWord16be 1
    put MultiFetchRequestType   = C.putWord16be 2
    put MultiProduceRequestType = C.putWord16be 3
    put OffsetsRequestType      = C.putWord16be 4

    get = C.getWord16be >>= \i -> case i of
            0 -> return ProduceRequestType
            1 -> return FetchRequestType
            2 -> return MultiFetchRequestType
            3 -> return MultiProduceRequestType
            4 -> return OffsetsRequestType
            _ -> fail $ "Unknown request type: " ++ show i

-- | Represents a Kafka topic.
newtype Topic = Topic ByteString
    deriving (Show, Read, Eq, Ord)

instance C.Serialize Topic where
    put (Topic topic) = do
        C.putWord16be (fromIntegral $ B.length topic)
        C.putByteString topic
    get = do
        topicLength <- fromIntegral <$> C.getWord16be
        Topic <$> C.getByteString topicLength

newtype Offset = Offset Word64
    deriving (Show, Read, Eq, Ord)

instance C.Serialize Offset where
    put (Offset o) = C.putWord64be o
    get = Offset <$> C.getWord64be

newtype Partition = Partition Word32
    deriving (Show, Read, Eq, Ord)

instance C.Serialize Partition where
    put (Partition p) = C.putWord32be p
    get = Partition <$> C.getWord32be

data Message = Message {
    messageCompression :: !Compression
  , messagePayload     :: {-# UNPACK #-} !ByteString
  } deriving (Show, Read, Eq, Ord)

-- TODO:
-- Should the Message constructor be exposed?
-- Should the caller be responsible for compressing the data?
-- We can use zlib and snappy to compress and decompress if necessary.

instance C.Serialize Message where
    put (Message compression payload) = do
        C.putWord32be (fromIntegral messageLength)
        C.putWord8 1 -- Magic
        C.put compression
        C.putWord32be (crc32 payload)
        C.putByteString payload
      where
        messageLength = B.length payload
            + 4 -- checksum
            + 1 -- compression
            + 1 -- magic

    get = C.getWord32be >>= \messageLength -> do
        magic <- C.getWord8
        compression <- case magic of
            0 -> return NoCompression
            1 -> C.get
            _ -> fail $ "Unknown magic code: " ++ show magic
        checksum <- C.getWord32be
        let remainingLength = fromIntegral $
              messageLength -
                ( 4                  -- checksum
                + 1                  -- magic
                + fromIntegral magic -- compression (1 byte if magic is 1)
                )
        payload <- C.getByteString remainingLength
        if crc32 payload == checksum
          then return (Message compression payload)
          else fail "Checksum did not match."


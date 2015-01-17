{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverlappingInstances       #-}
module Kafka.Internal.Types
    ( Error(..)
    , Compression(..)
    , OffsetsTime(..)
    , RequestType(..)

    , Topic(..)
    , Offset(..)
    , Partition(..)
    , Size(..)
    , Count(..)

    , Message(..)
    , MessageSet(..)
    ) where

import Control.Applicative
import Data.ByteString       (ByteString)
import Data.ByteString.Lazy  (fromStrict, toStrict)
import Data.Digest.CRC32     (crc32)
import Data.DList            (DList)
import Data.Monoid
import Data.String
import Data.Time             (UTCTime)
import Data.Time.Clock.POSIX
import Data.Word

import qualified Codec.Compression.GZip   as GZip
import qualified Codec.Compression.Snappy as Snappy
import qualified Data.ByteString          as B
import qualified Data.DList               as DList
import qualified Data.Serialize           as C

-- | Different errors returned by Kafka.
data Error
    = UnknownError          -- -1
    -- ^ Unknown error
    | OffsetOutOfRangeError --  1
    -- ^ Offset requested is invalid or no longer available on the server.
    | InvalidMessageError   --  2
    -- ^ A message failed to match its checksum.
    | WrongPartitionError   --  3
    -- ^ The requested partition doesn't exist.
    | InvalidFetchSizeError --  4
    -- ^ The maximum size requested for fetching is smaller than the message
    -- being fetched.
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

-- | Methods of compression supported by Kafka.
data Compression
    = NoCompression
    -- ^ The message is uncompressed.
    | GzipCompression
    -- ^ The message is compressed using @gzip@ compression and may contain
    -- other messages in it.
    | SnappyCompression
    -- ^ The message is compressed using @snappy@ compression and may contain
    -- other messages in it.
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

-- | Different times for which offsets may be retrieved using
-- 'Kafka.offsets'.
data OffsetsTime
    = OffsetsLatest
    -- ^ Retrieve the latest offsets
    | OffsetsEarliest
    -- ^ Retrieve the earliest offsets.
    | OffsetsBefore !UTCTime
    -- ^ Retrieve offsets before the given time.
    --
    -- Keep in mind that the response will not contain the precise offset that
    -- occurred around this time. It will return up to the specified count of
    -- offsets in descending, each being the first offset of every segment
    -- file for the specified partition with a modified time less than this
    -- time, and possibly a "high water mark" for the last segment of the
    -- partition (if it was modified before this time) which specifies the
    -- offset at which the next message to that partition will be written.
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

-- | The different request types supported by Kafka.
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
--
-- This is an instance of 'IsString' so a literal string may be used to create
-- a Topic with the @OverloadedStrings@ extension.
newtype Topic = Topic ByteString
    deriving (Show, Read, Eq, Ord, IsString)

instance C.Serialize Topic where
    put (Topic topic) = do
        C.putWord16be (fromIntegral $ B.length topic)
        C.putByteString topic
    get = do
        topicLength <- fromIntegral <$> C.getWord16be
        Topic <$> C.getByteString topicLength

-- | Represents an Offset in Kafka.
--
-- This is an instance of 'Num' so a literal number may be used to create an
-- Offset.
newtype Offset = Offset Word64
    deriving (Show, Read, Eq, Ord, Num)

instance C.Serialize Offset where
    put (Offset o) = C.putWord64be o
    get = Offset <$> C.getWord64be

-- | Represents a Kafka topic partition.
--
-- This is an instance of 'Num' so a literal number may be used to create a
-- Partition.
newtype Partition = Partition Word32
    deriving (Show, Read, Eq, Ord, Num)

instance C.Serialize Partition where
    put (Partition p) = C.putWord32be p
    get = Partition <$> C.getWord32be

-- | Represents a size.
--
-- This is an instance of 'Num' so a literal number may be used to create a
-- Size.
newtype Size = Size Word32
    deriving (Show, Read, Eq, Ord, Num)

instance C.Serialize Size where
    put (Size s) = C.putWord32be s
    get = Size <$> C.getWord32be

-- | Represents a Count.
--
-- This is an instance of 'Num' so a literal number may be used to create a
-- Size.
newtype Count = Count Word32
    deriving (Show, Read, Eq, Ord, Num)

instance C.Serialize Count where
    put (Count s) = C.putWord32be s
    get = Count <$> C.getWord32be

-- | Represents a Message being sent through Kafka.
data Message = Message {
    messageCompression :: !Compression
  -- ^ Compression used for the message.
  --
  -- If this is anything but 'NoCompression', this message contains other
  -- messages in it.
  , messagePayload     :: !ByteString
  -- ^ Message payload.
  --
  -- If the message is using any compression scheme, the payload contains
  -- other messages in the same format.
  } deriving (Show, Read, Eq, Ord)

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
            -- TODO: This should probably be InvalidMessageError

-- | Represents a collection of message payloads.
--
-- These are compressed into a single message using Snappy compression when
-- being sent.
newtype MessageSet = MessageSet { fromMessageSet :: [ByteString] }
    deriving (Show, Read, Eq, Monoid)

instance C.Serialize MessageSet where
    put (MessageSet messages) = C.put (Message SnappyCompression payload)
      where
        payload = Snappy.compress . C.runPut $
                    mapM_ (C.put . Message NoCompression) messages

    get = MessageSet . DList.toList <$> (C.get >>= readMessages)
      where
        readMessages :: Message -> C.Get (DList ByteString)
        readMessages (Message NoCompression payload) =
            return (DList.singleton payload)
        readMessages (Message compression payload) = do
            messages <- either fail return $
                        C.runGet (many C.get) decompressedPayload
            mconcat <$> mapM readMessages messages
          where
            decompressedPayload = decompress payload
            decompress = case compression of
              NoCompression -> id
              SnappyCompression -> Snappy.decompress
              GzipCompression -> toStrict . GZip.decompress . fromStrict

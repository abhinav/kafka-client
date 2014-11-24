{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
module Kafka.V07 where

import Control.Applicative
import Data.ByteString           (ByteString)
import Data.Digest.CRC32         (crc32)
import Data.Time                 (UTCTime)
import Data.Time.Clock.POSIX     (utcTimeToPOSIXSeconds)
import Data.Word                 (Word32, Word64)
import Network.Socket.ByteString (recv)

import qualified Control.Exception     as E
import qualified Data.ByteString       as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Serialize        as C
import qualified Network.Socket        as N

-- TODO break into modules
-- TODO This probably belongs in Kafka.V07.Internal or something.
--      V07 should expose a friendlier interface. Possibly similar to V08 so
--      that the top-level Kafka module just exports one of them.

-- | A connection to Kafka.
data Connection = Connection {
    connAddrInfo :: N.AddrInfo
  , connSocket   :: N.Socket
  }

-- | Create a new connection.
--
-- Connects to the given hostname and port. Throws an 'N.IOException' in case
-- of failure.
connect :: ByteString -> Int -> IO Connection
connect host port = do
    -- TODO Accept a config instead. Config will specify the buffer size.
    -- Connection can contain the send and recv functions based on buffer
    -- size.
    (addrInfo:_) <- N.getAddrInfo
                        (Just hints)
                        (Just $ B8.unpack host)
                        (Just $ show port)
    socket <- N.socket N.AF_INET N.Stream N.defaultProtocol
    N.connect socket (N.addrAddress addrInfo) `E.onException` N.close socket
    return $ Connection addrInfo socket
  where
    hints = N.defaultHints {
        N.addrFamily = N.AF_INET
      , N.addrFlags = [N.AI_NUMERICSERV]
      , N.addrSocketType = N.Stream
      }

-- | Close a connection.
close :: Connection -> IO ()
close Connection{connSocket} = N.close connSocket

-- | Open a connection, execute the given operation on it, and ensure it is
-- closed afterwards even if an exception was thrown.
withConnection :: ByteString -> Int -> (Connection -> IO a) -> IO a
withConnection host port = E.bracket (connect host port) close

-- | Receive the next response from the connection.
--
-- The response is either an 'Error' or a 'ByteString' containing the response
-- body.
recvResponse :: Connection -> IO (Response ByteString)
recvResponse Connection{connSocket} = do
    header <- recv connSocket 6
    -- TODO Maybe use something like a ProtocolError for parse errors.
    (respLen, err) <- either fail return $
        C.runGet ((,) <$> getLength
                      <*> getError) header
    body <- recv connSocket (respLen - 2)
    return $ maybe (Right body) Left err
  where
    getLength = fromIntegral <$> C.getWord32be

data Error
    = UnknownError
    | OffsetOutOfRangeError
    | InvalidMessageError
    | WrongPartitionError
    | InvalidFetchSizeError
  deriving (Show, Read, Eq)

-- | Reader for 'Error's.
--
-- Produces 'Nothing' if there was no error.
getError :: C.Get (Maybe Error)
getError = do
    errorCode <- C.getWord16be
    case errorCode of
        0  -> return Nothing
        1  -> return $ Just OffsetOutOfRangeError
        2  -> return $ Just InvalidMessageError
        3  -> return $ Just WrongPartitionError
        4  -> return $ Just InvalidFetchSizeError
        -1 -> return $ Just UnknownError
        _  ->   fail $ "Unknown error code: " ++ show errorCode

newtype Topic = Topic ByteString
    deriving (Show, Read, Eq)

instance C.Serialize Topic where
    put (Topic topic) = do
        C.putWord16be (fromIntegral $ B.length topic)
        C.putByteString topic
    get = do
        topicLength <- fromIntegral <$> C.getWord16be
        Topic <$> C.getByteString topicLength

newtype Offset = Offset Word64
    deriving (Show, Read, Eq, C.Serialize)

newtype Partition = Partition Word32
    deriving (Show, Read, Eq, C.Serialize)

data Request a = Request Topic Partition a
type Response a = Either Error a

data Compression
    = NoCompression
    | GzipCompression
    | SnappyCompression
  deriving (Show, Read, Eq)

instance C.Serialize Compression where
    put NoCompression = C.putWord8 0
    put GzipCompression = C.putWord8 1
    put SnappyCompression = C.putWord8 2

    get = C.getWord8 >>= \i -> case i of
        0 -> return NoCompression
        1 -> return GzipCompression
        2 -> return SnappyCompression
        _ -> fail $ "Invalid compression code: " ++ show i

-- TODO change to not use record fields. Hide Message constructor from user.
data Message = Message {
    messageCompression :: {-# UNPACK #-} !Compression
  , messagePayload     :: {-# UNPACK #-} !ByteString
  } deriving (Show, Read, Eq)

instance C.Serialize Message where
    put message = do
        C.putWord32be (fromIntegral messageLength)
        C.putWord8 1 -- Magic
        C.put compression
        C.putWord32be checksum
        C.putByteString payload
      where
        Message {
            messageCompression = compression
          , messagePayload = payload
          } = message
        checksum = crc32 payload
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
        _ <- C.getWord32be -- checksum ignored
        let remainingLength = messageLength -
                ( 4 -- checksum
                + 1 -- magic
                + (if magic == 0 then 0 else 1) -- compression
                )
        payload <- C.getByteString (fromIntegral remainingLength)
        return $ Message compression payload

data OffsetsTime
    = OffsetsLatest
    | OffsetsEarliest
    | OffsetsBefore UTCTime
  deriving (Show, Read, Eq)

putOffsetsTime :: C.Putter OffsetsTime
putOffsetsTime OffsetsLatest = C.putWord64be (-1)
putOffsetsTime OffsetsEarliest = C.putWord64be (-2)
putOffsetsTime (OffsetsBefore t) =
    C.putWord64be . round . (* 1000) $ utcTimeToPOSIXSeconds t

newtype Produce = Produce {
    produceMessages :: [Message]
  } deriving (Show, Read, Eq)

data Fetch = Fetch {
    fetchOffset  :: Offset
  , fetchMaxSize :: Word32
  } deriving (Show, Read, Eq)

data Offsets = Offsets {
    offsetsTime      :: OffsetsTime
  , offsetsMaxNumber :: Word32
  } deriving (Show, Read, Eq)

produce :: Connection -> [Request Produce] -> IO (Response ())
produce = undefined

fetch :: Connection -> [Request Fetch] -> IO (Response [Message])
fetch = undefined

offsets :: Connection -> Request Offsets -> IO (Response [Offset])
offsets = undefined

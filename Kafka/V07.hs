{-# LANGUAGE NamedFieldPuns #-}
module Kafka.V07 where

import Control.Applicative
import Data.ByteString           (ByteString)
import Data.Word                 (Word32)
import Network.Socket.ByteString (recv)

import qualified Control.Exception     as E
import qualified Data.ByteString.Char8 as B8
import qualified Data.Serialize        as C
import qualified Network.Socket        as N

import Kafka.V07.Internal

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
                      <*> C.get) header
    body <- recv connSocket (respLen - 2)
    return $ maybe (Right body) Left err
  where
    getLength = fromIntegral <$> C.getWord32be

data Request a = Request Topic Partition a
type Response a = Either Error a

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

-- TODO Use some sort of typeclass here to define data types which support
-- sending requests through them. The typeclass can provide the send/recv
-- methods necessary. That way, buffer size, etc. is hidden away in
-- Connection. This will also allow writing dummy implementations for tests.

produce :: Connection -> [Request Produce] -> IO (Response ())
produce = undefined

fetch :: Connection -> [Request Fetch] -> IO (Response [Message])
fetch = undefined

offsets :: Connection -> Request Offsets -> IO (Response [Offset])
offsets = undefined

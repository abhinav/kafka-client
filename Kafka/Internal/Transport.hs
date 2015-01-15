{-# LANGUAGE NamedFieldPuns #-}
module Kafka.Internal.Transport
    ( Transport(..)
    , recvExactly

    , Socket
    , withConnection
    , connect
    , close
    ) where


import Control.Applicative
import Data.ByteString     (ByteString)
import Network.Socket      (Socket, close)

import qualified Control.Exception         as E
import qualified Data.ByteString           as B
import qualified Data.ByteString.Char8     as B8
import qualified Network.Socket            as N
import qualified Network.Socket.ByteString as S

-- | Types that provide a means to send and receive bytes.
class Transport t where
    -- | Send the given ByteString down the transport.
    --
    -- This must block until the request has been finished.
    send :: t -> ByteString -> IO ()
    -- | Read up to the given number of bytes from the stream.
    --
    -- The returned ByteString may be empty if the end of the stream was
    -- reached.
    recv :: t -> Int -> IO ByteString

instance Transport Socket where
    send = S.sendAll
    recv = S.recv

-- | Keep reading from the given transport until the given number of bytes are
-- read or the end of the stream is reached -- whichever comes first.
recvExactly :: Transport t => t -> Int -> IO ByteString
recvExactly t size = B.concat . reverse <$> loop [] 0
  where
    loop chunks bytesRead
        | bytesRead >= size = return chunks
        | otherwise = do
            chunk <- recv t (size - bytesRead)
            if B.null chunk
              then return chunks
              else loop (chunk:chunks) $! bytesRead + B.length chunk
 -- TODO this should return a bytestring of exactly the given size. Anything
 -- else should be an IO exception or a protocol error since we use this only
 -- in cases where we know the exact size of the response.

getAddrInfo :: ByteString -> Int -> IO N.AddrInfo
getAddrInfo host port = head <$>
    N.getAddrInfo (Just hints)
                  (Just $ B8.unpack host)
                  (Just $ show port)
  where
    hints = N.defaultHints {
        N.addrFamily = N.AF_INET
      , N.addrFlags = [N.AI_NUMERICSERV]
      , N.addrSocketType = N.Stream
      }

-- | Create a new connection.
--
-- Connects to the given hostname and port. Throws an 'N.IOException' in case
-- of failure.
connect :: ByteString -> Int -> IO Socket
connect host port = do
    -- TODO Accept a config instead. Config will specify the buffer size.
    addrInfo <- getAddrInfo host port
    socket <- N.socket N.AF_INET N.Stream N.defaultProtocol
    N.connect socket (N.addrAddress addrInfo) `E.onException` N.close socket
    return socket

-- | Open a connection, execute the given operation on it, and ensure it is
-- closed afterwards even if an exception was thrown.
--
-- > withConnection "localhost" 9092 $ \conn ->
-- >    doStuff conn
-- >    fail "something went wrong"
--
-- Throws an 'IOException' if we were unable to open the connection.
--
withConnection :: ByteString -> Int -> (Socket -> IO a) -> IO a
withConnection host port = E.bracket (connect host port) close

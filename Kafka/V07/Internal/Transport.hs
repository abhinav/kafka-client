module Kafka.V07.Internal.Transport
    ( Transport(..)
    , recvExactly
    ) where

import Control.Applicative
import Data.ByteString     (ByteString)
import Network.Socket      (Socket)

import qualified Data.ByteString           as B
import qualified Network.Socket.ByteString as S

-- | Instantiated by types that provide a means to send and receive data.
class Transport t where
    -- | Send the given ByteString down the transport.
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

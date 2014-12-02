module Kafka.V07.Internal.Transport (Transport(..)) where

import Data.ByteString (ByteString)
import Network.Socket  (Socket)

import qualified Network.Socket.ByteString as S

-- | Instantiated by types that provide a means to send and receive data.
class Transport t where
    -- | Send the given ByteString down the transport.
    send :: t -> ByteString -> IO ()
    -- | Receive the given number of bytes from the transport.
    --
    -- The returned ByteString may be empty or smaller than the number of
    -- bytes requested if the end of the stream is reached.
    recv :: t -> Int -> IO ByteString

instance Transport Socket where
    send = S.sendAll
    recv = S.recv

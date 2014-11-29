{-# LANGUAGE NamedFieldPuns #-}
module Kafka.V07.Internal.Connection (
      Connection(..)
    , withConnection
    , connect
    , close
    ) where

import Control.Applicative
import Data.ByteString     (ByteString)

import qualified Control.Exception     as E
import qualified Data.ByteString.Char8 as B8
import qualified Network.Socket        as N

-- | A connection to Kafka.
data Connection = Connection {
    connAddrInfo :: N.AddrInfo
  , connSocket   :: N.Socket
  }

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
connect :: ByteString -> Int -> IO Connection
connect host port = do
    -- TODO Accept a config instead. Config will specify the buffer size.
    addrInfo <- getAddrInfo host port
    socket <- N.socket N.AF_INET N.Stream N.defaultProtocol
    N.connect socket (N.addrAddress addrInfo) `E.onException` N.close socket
    return $ Connection addrInfo socket

-- | Close a connection.
close :: Connection -> IO ()
close Connection{connSocket} = N.close connSocket

-- | Open a connection, execute the given operation on it, and ensure it is
-- closed afterwards even if an exception was thrown.
withConnection :: ByteString -> Int -> (Connection -> IO a) -> IO a
withConnection host port = E.bracket (connect host port) close



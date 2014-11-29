{-# LANGUAGE NamedFieldPuns #-}
module Kafka.V07 where

import Control.Applicative
import Data.ByteString           (ByteString)
import Data.Word                 (Word32)
import Network.Socket.ByteString (recv)

import qualified Data.Serialize as C

import Kafka.V07.Internal

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

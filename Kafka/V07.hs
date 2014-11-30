{-# LANGUAGE NamedFieldPuns #-}
module Kafka.V07
    (
      Connection
    , withConnection

    , Produce(..)
    , Fetch(..)
    , Offsets(..)
    , Response

    , produce
    , fetch
    , offsets

    , Message(..)

    , Topic(..)
    , Offset(..)
    , Partition(..)

    , Error(..)
    , Compression(..)
    , OffsetsTime(..)
    ) where

import Control.Applicative
import Control.Monad
import Network.Socket.ByteString (recv, sendAll)

import qualified Data.Serialize as C

import Kafka.V07.Internal

type Response a = Either Error a

-- | Receive the next response from the connection.
--
-- The response is either an 'Error' or a 'ByteString' containing the response
-- body.
recvResponse
    :: Connection
    -> C.Get a
    -> IO (Response a)
recvResponse Connection{connSocket} parse = do
    -- TODO use protocol error or something for parse errors instead of this
    -- TODO maybe also catch IO exceptions
    respLen <- C.runGet getLength <$> recv connSocket 4 >>= either fail return
    response <- recv connSocket respLen
    (err, body) <- either fail return
                 $ C.runGet ((,) <$> C.get <*> parse) response
    case err of
        Just e -> return (Left e)
        Nothing -> return (Right body)
  where
    getLength = fromIntegral <$> C.getWord32be

-- TODO Use some sort of typeclass here to define data types which support
-- sending requests through them. The typeclass can provide the send/recv
-- methods. That way, buffer size and other transport-specific things are
-- hidden away. This will also allow writing dummy transports for tests.

produce :: Connection -> [Produce] -> IO ()
produce _ [] = return ()
produce Connection{connSocket} reqs =
    sendAll connSocket . C.runPut $
        case reqs of
            [x] -> putProduceRequest x
            xs  -> putMultiProduceRequest xs

-- this should probably be a list of lists -- a list of messages for each
-- fetch request.
fetch :: Connection -> [Fetch] -> IO (Response [Message])
fetch _ [] = return (Right [])
fetch conn@Connection{connSocket} reqs = do
    sendAll connSocket . C.runPut $
        case reqs of
            [x] -> putFetchRequest x
            xs -> putMultiFetchRequest xs
    recvResponse conn (many C.get)

offsets :: Connection -> Offsets -> IO (Response [Offset])
offsets conn@Connection{connSocket} req = do
    sendAll connSocket . C.runPut $ putOffsetsRequest req
    recvResponse conn $ do
        count <- C.getWord32be
        replicateM (fromIntegral count) C.get

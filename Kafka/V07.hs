{-# LANGUAGE NamedFieldPuns #-}
module Kafka.V07
    (
      Connection
    , withConnection
    , Transport(..)

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
import Data.ByteString.Lazy (fromStrict, toStrict)

import qualified Codec.Compression.GZip   as GZip
import qualified Codec.Compression.Snappy as Snappy
import qualified Data.Serialize           as C

import Kafka.V07.Internal

type Response a = Either Error a

-- | Receive the next response from the connection.
--
-- The response is either an 'Error' or a 'ByteString' containing the response
-- body.
recvResponse
    :: Transport t
    => t
    -> C.Get a
    -> IO (Response a)
recvResponse transport parse = do
    -- TODO use protocol error or something for parse errors instead of this
    -- TODO maybe also catch IO exceptions
    respLen <- C.runGet getLength <$> recv transport 4 >>= either fail return
    response <- recv transport respLen
    (err, body) <- either fail return
                 $ C.runGet ((,) <$> C.get <*> parse) response
    case err of
        Just e -> return (Left e)
        Nothing -> return (Right body)
  where
    getLength = fromIntegral <$> C.getWord32be

produce :: Transport t => t -> [Produce] -> IO ()
produce _ [] = return ()
produce transport reqs =
    send transport . C.runPut $
        case reqs of
            [x] -> putProduceRequest x
            xs  -> putMultiProduceRequest xs

-- this should probably be a list of lists -- a list of messages for each
-- fetch request.
fetch :: Transport t => t -> [Fetch] -> IO (Response [Message])
fetch _ [] = return (Right [])
fetch transport reqs = do
    send transport . C.runPut $
        case reqs of
            [x] -> putFetchRequest x
            xs -> putMultiFetchRequest xs
    fmap (concatMap decodeMessages) <$> recvResponse transport (many C.get)

offsets :: Transport t => t -> Offsets -> IO (Response [Offset])
offsets transport req = do
    send transport . C.runPut $ putOffsetsRequest req
    recvResponse transport $ do
        count <- C.getWord32be
        replicateM (fromIntegral count) C.get

decodeMessages :: Message -> [Message]
decodeMessages msg@(Message NoCompression _) = [msg]
decodeMessages (Message SnappyCompression payload) =
    either error (concatMap decodeMessages) $
    parse (Snappy.decompress payload)
  where
    parse = C.runGet (many C.get)
decodeMessages (Message GzipCompression payload) =
    either error (concatMap decodeMessages) $
    parse (gzipDecompress payload)
  where
    gzipDecompress = toStrict . GZip.decompress . fromStrict
    parse = C.runGet (many C.get)

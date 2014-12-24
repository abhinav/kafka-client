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

    , Topic(..)
    , Offset(..)
    , Partition(..)
    , Size(..)
    , Count(..)

    , Error(..)
    , Compression(..)
    , OffsetsTime(..)
    ) where

import Control.Applicative
import Data.Monoid

import qualified Data.Serialize as C

import Kafka.V07.Internal

-- | Receive the next response from the connection.
--
-- The response is either an 'Error' or a 'ByteString' containing the response
-- body.
recvResponse
    :: Transport t
    => t
    -> C.Get a
    -> IO (Response a)
recvResponse transport getBody = do
    -- TODO use protocol error or something for parse errors instead of this
    -- TODO maybe also catch IO exceptions
    respLen <- C.runGet getLength <$> recv transport 4 >>= either fail return
    response <- recvExactly transport respLen
    either fail return $ C.runGet getResponse response
  where
    getLength = fromIntegral <$> C.getWord32be
    getResponse = do
        err <- C.get
        body <- getBody
        case err of
            Just e -> return (Left e)
            Nothing -> return (Right body)

produce :: Transport t => t -> [Produce] -> IO ()
produce transport reqs = case reqs of
  []  -> return ()
  [x] -> send transport (C.runPut $ putProduceRequest x)
  xs  -> send transport (C.runPut $ putMultiProduceRequest xs)

fetch :: Transport t => t -> [Fetch] -> IO (Response [FetchResponse])
fetch transport reqs = case reqs of
  [] -> return (Right mempty)
  [x] -> do
    send transport (C.runPut $ putFetchRequest x)
    fmap (:[]) <$> recvResponse transport getFetchResponse
  xs -> do
    send transport (C.runPut $ putMultiFetchRequest xs)
    recvResponse transport (getMultiFetchResponse $ length xs)

offsets :: Transport t => t -> Offsets -> IO (Response [Offset])
offsets transport req = do
    send transport (C.runPut $ putOffsetsRequest req)
    recvResponse transport getOffsetsResponse

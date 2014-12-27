{-# LANGUAGE NamedFieldPuns #-}
module Kafka.V07
    (

    -- * Primary interface

      withConnection
    , produce
    , fetch
    , offsets

    -- ** Types

    , Produce(..)

    , Fetch(..)
    , FetchResponse(..)

    , Offsets(..)
    , OffsetsTime(..)

    -- * Common types

    , Topic(..)
    , Offset(..)
    , Partition(..)
    , Size(..)
    , Count(..)

    -- * Other

    , Connection
    , Transport(..)
    , Error(..)
    , Response
    ) where

import Control.Applicative
import Data.Monoid

import qualified Data.Serialize as C

import Kafka.V07.Internal

-- | Receive the next response from the connection using the given
-- deserializer.
--
-- Returns either an 'Error' for Kafka errors or the deseriazlied result.
recvResponse
    :: Transport t
    => t        -- ^ Transport from which the response will be read.
    -> C.Get a  -- ^ Deserializer
    -> IO (Response a)
recvResponse transport getBody = do
    -- TODO use protocol error or something for parse errors instead of this
    -- TODO maybe also catch IO exceptions
    -- TODO alternatively, throw protocol and IO exceptions instead of failing
    -- like this.
    respLen <- C.runGet getLength <$> recv transport 4 >>= either fail return
    response <- recvExactly transport respLen
    either fail return $ C.runGet getResponse response
  where
    getLength = fromIntegral <$> C.getWord32be
    getResponse = do
        err <- C.get
        body <- getBody
        return $ maybe (Right body) Left err

-- | Send the given 'Produce' requests to Kafka.
--
-- @
-- 'withConnection' "localhost" 9092 $ \conn ->
--   produce conn [
--      'Produce' ('Topic' "my-topic") ('Partition' 0) ["hello"]
--    , 'Produce' "another-topic" 0 ["world"]
--    ]
-- @
produce :: Transport t => t -> [Produce] -> IO ()
produce transport reqs = case reqs of
  []  -> return ()
  [x] -> send transport (C.runPut $ putProduceRequest x)
  xs  -> send transport (C.runPut $ putMultiProduceRequest xs)

-- | Fetch messages from Kafka.
--
-- Each 'Fetch' request can fetch messages from different Kafka
-- topic-partition pairs.
--
-- Returns a list of 'FetchResponse's in the same order as the 'Fetch'
-- requests. Each response contains the messages returned for the
-- corresponding request and the new offset at which the next request should
-- be made for that topic-partition pair to get the messages that follow.
--
-- @
-- 'withConnection' "localhost" 9092 $ \conn -> do
--   Right ['FetchResponse' messages newOffset] <- fetch conn [
--       'Fetch' ('Topic' "test-topic") ('Partition' 0) someOffset 1024
--     ]
--   doStuff messages
--   fetch conn ['Fetch' "test-topic" 0 newOffset 1024] >>= doSomething
-- @
fetch :: Transport t => t -> [Fetch] -> IO (Response [FetchResponse])
fetch transport reqs = case reqs of
  [] -> return (Right mempty)
  [x] -> do
    send transport (C.runPut $ putFetchRequest x)
    fmap (:[]) <$> recvResponse transport (getFetchResponse x)
  xs -> do
    send transport (C.runPut $ putMultiFetchRequest xs)
    recvResponse transport (getMultiFetchResponse xs)

-- | Retrieve offsets from Kafka.
--
-- @
-- 'withConnection' "localhost" 9092 $ \conn -> do
--   Right [o] <- offsets conn (Offsets "topic" 0 OffsetsEarliest 1)
--   fetch conn ['Fetch' "topic" 0 o 1024] >>= doSomething
-- @
offsets :: Transport t => t -> Offsets -> IO (Response [Offset])
offsets transport req = do
    send transport (C.runPut $ putOffsetsRequest req)
    recvResponse transport getOffsetsResponse

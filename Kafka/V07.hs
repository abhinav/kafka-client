{-# LANGUAGE NamedFieldPuns #-}
-- |
-- Module      :  Kafka.V07
-- Copyright   :  Abhinav Gupta 2015
-- License     :  MIT
--
-- Maintainer  :  mail@abhinavg.net
-- Stability   :  experimental
-- Portability :  GHC
--
-- A library to interact with Apache Kafka 0.7.
--
module Kafka.V07
    (
    -- * Main interface
    --
    -- | Requests to Kafka can be made using 'produce', 'fetch', and
    -- 'offsets'.  For 'produce' and 'fetch', the functions automatically
    -- decide whether the request needs to be a single @Produce@/@Fetch@
    -- request or a @Multi*@ request.
    --
    -- The request operations send requests and receive responses using any
    -- type that is an instance of 'Transport'. 'withConnection' produces one
    -- such object.
    --
      withConnection
    , produce
    , fetch
    , offsets

    -- * Types

    , Produce(..)

    , Fetch(..)
    , FetchResponse(..)

    , Offsets(..)
    , OffsetsTime(..)

    , Topic(..)
    , Offset(..)
    , Partition(..)
    , Size(..)
    , Count(..)

    -- * Other

    , Error(..)
    , Response

    -- * Transport

    , Socket
    , Transport(..)
    ) where

import Control.Applicative
import Data.Monoid

import qualified Data.Serialize as C

import Kafka.V07.Internal

-- | Receives the next response from the connection using the given
-- deserializer.
--
-- Returns either an 'Error' for Kafka errors or the deseriazlied result.
recvResponse
    :: Transport t
    => t        -- ^ Transport from which the response will be read.
    -> C.Get a  -- ^ Deserializer
    -> IO (Response a)
recvResponse transport getBody = do
    -- TODO this probably belongs in an internal module somewhere
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

-- | Sends the given 'Produce' requests to Kafka.
--
-- If multiple requests are supplied, a @MultiProduce@ request is made.
--
-- @
-- 'withConnection' \"localhost\" 9092 $ \\conn ->
--   produce conn [
--      'Produce' ('Topic' \"my-topic\") ('Partition' 0) [\"foo\"]
--    , Produce \"another-topic\" 0
--                [\"multiple\", \"messages\"]
--    ]
-- @
--
-- Note that string literals may be used in place of 'Topic' (with the
-- @OverloadedStrings@ GHC extension), and integer literals may be used in
-- place of 'Partition'.
produce :: Transport t => t -> [Produce] -> IO ()
produce transport reqs = case reqs of
  []  -> return ()
  [x] -> send transport (C.runPut $ putProduceRequest x)
  xs  -> send transport (C.runPut $ putMultiProduceRequest xs)

-- | 'Fetch'es messages from Kafka.
--
-- If multiple Fetch requests are supplied, a @MultiFetch@ request is made.
--
-- @
-- 'withConnection' \"localhost\" 9092 $ \\conn -> do
--   Right ['FetchResponse' messages newOffset] <- fetch conn [
--       'Fetch' ('Topic' \"test-topic\") ('Partition' 0) (Offset 42) 1024
--     ]
--   {- Consume the messages here -}
--   response <- fetch conn ['Fetch' \"test-topic\" 0 newOffset 1024]
--   {- ... -}
-- @
--
-- Returns a list of 'FetchResponse's in the same order as the 'Fetch'
-- requests. Each response contains the messages returned for the
-- corresponding request and the new offset at which the next request should
-- be made for that request to get the messages that follow.
--
-- If a response for a request contains no messages, the specified
-- topic-partition pair has been exhausted.
--
-- Note that string literals may be used in place of 'Topic' (with the
-- @OverloadedStrings@ GHC extension), and integer literals may be used in
-- place of 'Offset'.
--
fetch :: Transport t => t -> [Fetch] -> IO (Response [FetchResponse])
fetch transport reqs = case reqs of
  [] -> return (Right mempty)
  [x] -> do
    send transport (C.runPut $ putFetchRequest x)
    fmap (:[]) <$> recvResponse transport (getFetchResponse x)
  xs -> do
    send transport (C.runPut $ putMultiFetchRequest xs)
    recvResponse transport (getMultiFetchResponse xs)

-- | Retrieve message offsets from Kafka.
--
-- @
-- 'withConnection' \"localhost\" 9092 $ \\conn -> do
--   Right [o] <- offsets conn (Offsets \"topic\" 0 OffsetsEarliest 1)
--   fetch conn ['Fetch' \"topic\" 0 o 1024] >>= doSomething
-- @
--
-- Note that string literals may be used in place of 'Topic' (with the
-- @OverloadedStrings@ GHC extension), and integer literals may be used in
-- place of 'Count'.
--
offsets :: Transport t => t -> Offsets -> IO (Response [Offset])
offsets transport req = do
    send transport (C.runPut $ putOffsetsRequest req)
    recvResponse transport getOffsetsResponse

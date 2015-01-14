module Kafka.V07.Internal.Response
    ( Response
    , FetchResponse(..)
    , getFetchResponse
    , getMultiFetchResponse
    , getOffsetsResponse
    ) where

import Control.Applicative
import Control.Monad
import Data.ByteString     (ByteString)
import Data.Monoid

import qualified Data.Serialize as C

import Kafka.V07.Internal.Request
import Kafka.V07.Internal.Types

type Response a = Either Error a

-- | Result of a single Kafka 'Kafka.V07.Fetch'.
data FetchResponse = FetchResponse {
    fetchMessages  ::                [ByteString]
  -- ^ List of messages yielded for the 'Kafka.V07.Fetch' request.
  , fetchNewOffset :: {-# UNPACK #-} !Offset
  -- ^ New offset at which the next 'Fetch' request should start reading to
  -- get the messages that follow.
  } deriving (Show, Read, Eq)

getFetchResponse :: Fetch -> C.Get FetchResponse
getFetchResponse Fetch{fetchOffset = oldOffset} = do
    startBytes <- C.remaining
    messages <- fromMessageSet . mconcat <$> many C.get
    endBytes <- C.remaining
    let newOffset = oldOffset + fromIntegral (startBytes - endBytes)
    return $ FetchResponse messages $! newOffset

getMultiFetchResponse :: [Fetch] -> C.Get [FetchResponse]
getMultiFetchResponse requests = forM requests $ \request -> do
    size <- fromIntegral <$> C.getWord32be
    C.skip 2 -- skip error
    isolate' (size - 2) (getFetchResponse request)

getOffsetsResponse :: C.Get [Offset]
getOffsetsResponse = do
    numOffsets <- C.getWord32be
    replicateM (fromIntegral numOffsets) C.get

-- | A version of 'C.isolate' that does not fail the parse because of
-- unconsumed data.
isolate' :: Int -> C.Get a -> C.Get a
isolate' n m = do
    s <- C.getBytes n
    either fail return $ C.runGet m s

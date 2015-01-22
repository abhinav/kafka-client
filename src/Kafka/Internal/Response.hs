module Kafka.Internal.Response
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

import Kafka.Internal.Request
import Kafka.Internal.Types

-- | A response from Kafka can either be a failure or the value that was
-- expected.
type Response a = Either Error a

-- | Result of a single Kafka 'Kafka.Fetch'.
data FetchResponse = FetchResponse {
    fetchMessages  ::                [ByteString]
  -- ^ List of messages returned in the response for the 'Kafka.Fetch'
  -- request.
  , fetchNewOffset :: {-# UNPACK #-} !Offset
  -- ^ New offset at which the next 'Fetch' request should start reading in
  -- the same topic and partition to access the messages that follow the
  -- messages returned in this response.
  } deriving (Show, Read, Eq)

-- | Parses a single @FetchResponse@ for the given @Fetch@ request.
getFetchResponse :: Fetch -> C.Get FetchResponse
getFetchResponse Fetch{fetchOffset = oldOffset} = do
    startBytes <- C.remaining
    messages <- fromMessageSet . mconcat <$> many C.get
    endBytes <- C.remaining
    let newOffset = oldOffset + fromIntegral (startBytes - endBytes)
    return $ FetchResponse messages $! newOffset

-- | Parses @FetchResponse@s for each of the given @Fetch@ requests.
getMultiFetchResponse :: [Fetch] -> C.Get [FetchResponse]
getMultiFetchResponse requests = forM requests $ \request -> do
    size <- fromIntegral <$> C.getWord32be
    C.skip 2 -- skip error
    isolate' (size - 2) (getFetchResponse request)

-- | Parses the response for an @Offsets@ request.
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

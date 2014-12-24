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
import Data.Sequence       (Seq)

import qualified Data.Serialize as C

import Kafka.V07.Internal.Types

type Response a = Either Error a

-- TODO The response should probably be a list, and a DList while parsing
-- since it's never read from
data FetchResponse = FetchResponse {
    fetchMessages  :: Seq ByteString
  , fetchBytesRead :: Int
  } deriving (Show, Read, Eq)

getFetchResponse :: C.Get FetchResponse
getFetchResponse = do
    startBytes <- C.remaining
    messages <- fromMessageSet . mconcat <$> many C.get
    endBytes <- C.remaining
    return $ FetchResponse messages $! startBytes - endBytes

getMultiFetchResponse :: Int -> C.Get [FetchResponse]
getMultiFetchResponse numRequests = replicateM numRequests $ do
    size <- fromIntegral <$> C.getWord32be
    C.skip 2 -- skip error
    isolate' (size - 2) getFetchResponse

getOffsetsResponse :: C.Get [Offset]
getOffsetsResponse = do
    numOffsets <- C.getWord32be
    replicateM (fromIntegral numOffsets) C.get

-- | A version of 'C.isolate' that does not complain about unconsumed data.
isolate' :: Int -> C.Get a -> C.Get a
isolate' n m = do
    s <- C.getBytes n
    either fail return $ C.runGet m s

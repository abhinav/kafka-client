module Kafka.V07.Internal.Response where

import Control.Applicative
import Control.Monad
import Data.ByteString     (ByteString)
import Data.Monoid
import Data.Sequence       (Seq)

import qualified Data.Serialize as C

import Kafka.V07.Internal.Types

type Response a = Either Error a

getResponse :: C.Get a -> C.Get (Response a)
getResponse getBody = do
    err <- C.get
    body <- getBody
    case err of
        Just e -> return (Left e)
        Nothing -> return (Right body)

data FetchResponse = FetchResponse {
    fetchMessages  :: Seq ByteString
  , fetchBytesRead :: Int
  } deriving (Show, Read, Eq)

getFetchResponse :: C.Get FetchResponse
getFetchResponse = do
    startBytes <- C.remaining
    messages <- fromMessageSet . mconcat <$> many C.get
    endBytes <- C.remaining

    return FetchResponse{
        fetchMessages = messages
      , fetchBytesRead = startBytes - endBytes
      }

getMultiFetchResponse :: Int -> C.Get [FetchResponse]
getMultiFetchResponse numRequests = replicateM numRequests $ do
    size <- getLength
    C.skip 2 -- skip error
    C.isolate size getFetchResponse
  where
    getLength = fromIntegral <$> C.getWord32be

getOffsetsResponse :: C.Get [Offset]
getOffsetsResponse = do
    numOffsets <- C.getWord32be
    replicateM (fromIntegral numOffsets) C.get

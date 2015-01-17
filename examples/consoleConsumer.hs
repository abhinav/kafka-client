{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import           Control.Concurrent    (threadDelay)
import           Control.Monad
import qualified Data.ByteString.Char8 as B8
import           System.Environment
import           System.Exit

import Kafka

main :: IO ()
main = do
  args <- getArgs
  when (null args) $ do
    putStrLn "USAGE: consoleProducer TOPIC"
    exitFailure

  let topic = Topic (B8.pack $ head args)

  withConnection "localhost" 9092 $ \conn -> do
    startOffset:_ <- offsets conn (Offsets topic 0 OffsetsLatest 10)
                        >>= either (fail . show) return
    loop conn (Fetch topic 0 startOffset 1024)
  where
    loop conn fetchReq = do
        [FetchResponse{
            fetchMessages = messages
          , fetchNewOffset = newOffset
          }] <- fetch conn [fetchReq] >>= either (fail . show) return

        mapM_ B8.putStrLn messages
        when (null messages) $
            threadDelay (1000 * 1000)

        loop conn fetchReq{fetchOffset = newOffset}

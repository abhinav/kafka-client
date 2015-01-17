{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import           Control.Applicative
import           Control.Monad
import qualified Data.ByteString.Char8 as B8
import           System.Environment
import           System.Exit
import qualified System.IO             as IO

import Kafka

main :: IO ()
main = do
  args <- getArgs
  when (null args) $ do
    putStrLn "USAGE: consoleProducer TOPIC"
    exitFailure

  let topic = Topic (B8.pack $ head args)
  withConnection "localhost" 9092 $ \conn ->
    whileM_ (not <$> IO.isEOF) $ do
        line <- B8.getLine
        produce conn [Produce topic 0 [line]]

whileM_ :: Monad m => m Bool -> m a -> m ()
whileM_ cond action = loop
  where
    loop = do
        x <- cond
        when x (action >> loop)

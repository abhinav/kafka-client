{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Control.Exception
import Test.Hspec
import Test.Kafka.Managed

import qualified Network        as N
import qualified System.IO.Temp as Temp

import qualified Kafka
import qualified KafkaSpec

getRandomUnusedPort :: IO Int
getRandomUnusedPort =
    bracket (N.listenOn $ N.PortNumber 0) N.sClose $ \socket -> do
        (N.PortNumber num) <- N.socketPort socket
        return (fromIntegral num)

main :: IO ()
main = do
  kafkaPort <- getRandomUnusedPort
  Temp.withSystemTempDirectory "kafka-logs" $ \logDir ->
    let kafkaConfig = KafkaConfig {
            kafkaLogDirectory = logDir
          , kafkaServerPort = kafkaPort
          }
    in withManagedKafka kafkaConfig $
       Kafka.withConnection "localhost" kafkaPort $
           hspec . KafkaSpec.spec

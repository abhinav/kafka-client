{-# LANGUAGE RecordWildCards #-}
module Test.Kafka.Managed
    ( KafkaConfig(..)
    , withManagedKafka
    ) where

import Control.Applicative
import Control.Concurrent  (threadDelay)
import Control.Exception   (IOException, bracket, catch)
import System.Exit         (ExitCode (..))

import qualified Network.Socket as N
import qualified System.IO      as IO
import qualified System.IO.Temp as Temp
import qualified System.Process as P

data KafkaConfig = KafkaConfig {
    kafkaServerPort   :: Int
  , kafkaLogDirectory :: FilePath
  } deriving (Show, Eq)

toProperties :: KafkaConfig -> [(String, String)]
toProperties KafkaConfig{..} =
    [ ("brokerid", "0")
    , ("port", show kafkaServerPort)
    , ("log.dir", kafkaLogDirectory)
    , ("num.partitions", "1")
    , ("enable.zookeeper", "false")
    , ("log.flush.interval", "1")
    , ("log.default.flush.interval.ms", "10")
    , ("log.default.flush.scheduler.interval.ms", "5")
    ]

writeProperties :: [(String, String)] -> IO.Handle -> IO ()
writeProperties entries h = IO.hPutStr h . unlines $ do
    (key, value) <- entries
    return $! key ++ "=" ++ value

withManagedKafka :: KafkaConfig -> IO a -> IO a
withManagedKafka config m =
  Temp.withSystemTempFile "kafka.proeprties" $ \configPath handle -> do
    writeProperties (toProperties config) handle
    IO.hClose handle
    bracket (startServer configPath) stopServer . const $ do
        waitUntilConnectable (kafkaServerPort config)
        m

-- | Keep trying to connect to the given port until the server accepts a
-- connection or we've made more than 10 attempts.
waitUntilConnectable :: Int -> IO ()
waitUntilConnectable port = loop (0 :: Int)
  where
    loop attempts
        -- TODO maxAttempts can probably be a parameter
      | attempts >= 10 = error "Can't connect to Kafka after 10 attempts."
      | otherwise = do
          addrInfo <- head <$> N.getAddrInfo
                  (Just $ N.defaultHints {
                          N.addrFlags = [N.AI_NUMERICSERV]
                        , N.addrFamily = N.AF_INET
                        , N.addrSocketType = N.Stream
                        })
                  (Just "localhost") (Just $ show port)
          socket <- N.socket N.AF_INET N.Stream N.defaultProtocol
          let connect = do
                  N.connect socket (N.addrAddress addrInfo)
                  N.close socket
              retry = do
                  threadDelay (200 * 1000)
                  loop (attempts + 1)
          connect `catchIO` const retry
    catchIO :: IO a -> (IOException -> IO a) -> IO a
    catchIO = catch

-- TODO Better stdout/stderr handling. Could return a ManagedKafka object here
-- that allows access to the logs in case of failure.
startServer :: FilePath -> IO P.ProcessHandle
startServer configPath =
  IO.withFile "/dev/null" IO.WriteMode $ \nullHandle -> do
    (_, _, _, h) <- P.createProcess (p nullHandle)
    return h
  where
    p nullHandle = (P.proc "kafka-server-start.sh" [configPath]) {
        P.std_out = P.UseHandle nullHandle
      }

stopServer :: P.ProcessHandle -> IO ()
stopServer h = do
    P.interruptProcessGroupOf h
    code <- P.waitForProcess h
    case code of
      ExitSuccess -> return ()
      ExitFailure 130 -> return ()  -- Exit code 130 == terminated with Ctrl-C
      ExitFailure exitCode -> putStrLn . unwords $
          ["Failed to stop Kafka server. Exit code", show exitCode]


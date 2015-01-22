{-# LANGUAGE OverloadedStrings #-}
module KafkaSpec (spec) where

import Control.Applicative   ((<$>))
import Control.Concurrent    (threadDelay)
import Control.Monad         (replicateM)
import Data.IORef            (modifyIORef', newIORef, readIORef)
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

import qualified Data.Set as Set

import Kafka
import Test.Kafka.Arbitrary

-- | Keep running the given generator until a value that satisfies the given
-- predicate is produced.
generateSuchThat :: Gen a -> (a -> IO Bool) -> IO a
generateSuchThat gen f = loop
  where
    loop = do
      a <- generate gen
      match <- f a
      if match
        then return a
        else loop

-- | Returns an IO action that will always return unique values from the given
-- generator.
uniqueGenerator :: Ord a => Gen a -> IO (IO a)
uniqueGenerator gen = do
    generated <- newIORef Set.empty
    let isNew a = Set.notMember a <$> readIORef generated
    return $ do
        a <- generateSuchThat gen isNew
        modifyIORef' generated (Set.insert a)
        return a

spec :: Socket -> Spec
spec conn = describe "Kafka" $ do

    -- This is hacky but it will ensure that none of the tests run under the
    -- same Kafka topic.
    getNewTopic <- runIO $ uniqueGenerator arbitrary

    prop "returns zero offset for empty topic" $ do
        topic <- getNewTopic
        let req = Offsets topic 0 OffsetsLatest 10
        offsets conn req `shouldReturn` Right [0]

    prop "can produce and fetch empty payloads" $ do
        topic <- getNewTopic
        produce conn [Produce topic 0 [""]]
        Right resp <- pollMessages conn $ Fetch topic 0 0 (1024 * 1024)
        fetchMessages resp `shouldBe` [""]

    prop "can produce and fetch empty lists of payloads" $ do
        topic <- getNewTopic
        produce conn [Produce topic 0 []]
        fetch conn [fetchRequest topic 0]
            `shouldReturn` Right [FetchResponse [] 0]

    prop "can fetch and produce" $ \(NonEmptyPayloadList messages) -> do
        topic <- getNewTopic
        produce conn [Produce topic 0 messages]

        Right resp <- pollMessages conn (fetchRequest topic 0)
        fetchMessages resp `shouldBe` messages

        let newOffset = fetchNewOffset resp

        offsets conn (Offsets topic 0 OffsetsLatest 10)
            `shouldReturn` Right [newOffset, 0]

        fetch conn [fetchRequest topic newOffset]
            `shouldReturn` Right [FetchResponse [] newOffset]

    prop "can multi-fetch and multi-produce" $ \(NonEmpty ps) -> do
        topics <- replicateM (length ps) getNewTopic
        let messageSets = map getPayloads ps
            produceReqs = zipWith
                            (\t ms -> Produce t 0 ms) topics messageSets
            fetchReqs = map (`fetchRequest` 0) topics

            receivedAll (Left _) = False
            receivedAll (Right responses) = all isSuccess responses
              where
                isSuccess = not . null . fetchMessages

        produce conn produceReqs
        responses <- do
            result <- retryUntil receivedAll 10 500 (fetch conn fetchReqs)
            case result of
                Just (Right responses) -> return responses
                Just (Left e) -> fail (show e)
                Nothing -> fail "Failed to fetch messages."

        map fetchMessages responses `shouldBe` messageSets
  where
    fetchRequest topic offset = Fetch topic 0 offset (1024 * 1024)

-- | Polls the given transport with the given Fetch request until a non-empty
-- list of messages is available.
--
-- Throws an error if a non-empty list of messages is not received after
-- trying for 5 seconds.
pollMessages :: Transport t => t -> Fetch -> IO (Response FetchResponse)
pollMessages t req = do
    result <- retryUntil isSuccess 10 500 (fetch t [req])
    case result of
        Nothing -> fail "Failed to fetch messages."
        Just xs -> return $ head <$> xs
  where
    isSuccess (Left _) = False
    isSuccess (Right [resp]) = not . null . fetchMessages $ resp
    isSuccess (Right xs) =
        error $ "Too many messages in response: " ++ show xs

-- | Retries the given IO operation with the specified delay until the given
-- predicate on the result returns True.
--
-- Return Nothing if the maximum number of attempts was exceeded.
retryUntil :: (a -> Bool) -> Int -> Int -> IO a -> IO (Maybe a)
retryUntil predicate maxAttempts delayMillis m = loop 0
  where
    loop attempt
        | attempt >= maxAttempts = return Nothing
        | otherwise = do
            a <- m
            if predicate a
              then return (Just a)
              else threadDelay (delayMillis * 1000) >> loop (attempt + 1)


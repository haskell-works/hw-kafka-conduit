{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.Conduit.CombinatorSpec (spec) where

import Data.Conduit
import Data.Conduit.List as CL
import Data.List.Extra
import Kafka.Conduit.Combinator
import Test.Hspec
import Test.QuickCheck
import Prelude as P

{-# ANN module ("HLint: ignore Redundant do"        :: String) #-}
{-# ANN module ("HLint: ignore Reduce duplication"  :: String) #-}

spec :: Spec
spec = describe "Kafka.Conduit.UtilSpec" $ do
  describe "batchBy" $ do
    it "Should batch properly" $ do
      forAll (choose (1 :: Int, 100)) $ \i -> do
        xs :: [[Int]] <- runConduit $ sourceList [1..100] .| batchBy i .| CL.consume
        P.concat xs `shouldBe` [1..100]
        P.filter (/= i) (length <$> P.init xs) `shouldBe` []
  describe "batchByOrFlush" $ do
    it "Should batch by i properly when Nothing is interspersed between every j elements" $ do
      forAll (choose (1 :: Int, 100)) $ \i ->
        forAll (choose (1 :: Int, 100)) $ \j -> do
          let as = intercalate [Nothing] (chunksOf j (Just <$> [1..100]))
          xs :: [[Int]] <- runConduit $ sourceList as .| batchByOrFlush i .| CL.consume
          P.concat xs `shouldBe` [1..100]
          P.filter (> i) (length <$> P.init xs) `shouldBe` []
          P.filter (> j) (length <$> P.init xs) `shouldBe` []
    it "Should batch by i properly when [Nothing, Nothing] is interspersed between every j elements" $ do
      forAll (choose (1 :: Int, 100)) $ \i ->
        forAll (choose (1 :: Int, 100)) $ \j -> do
          let as = intercalate [Nothing, Nothing] (chunksOf j (Just <$> [1..100]))
          xs :: [[Int]] <- runConduit $ sourceList as .| batchByOrFlush i .| CL.consume
          P.concat xs `shouldBe` [1..100]
          P.filter (> i) (length <$> P.init xs) `shouldBe` []
          P.filter (> j) (length <$> P.init xs) `shouldBe` []

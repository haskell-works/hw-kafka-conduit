{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.Conduit.UtilsSpec (spec) where

import Data.Conduit
import Data.Conduit.List as CL
import Kafka.Conduit.Combinator
import Test.Hspec
import Test.QuickCheck
import Prelude as P

{-# ANN module ("HLint: ignore Redundant do"        :: String) #-}

spec :: Spec
spec = describe "Kafka.Conduit.UtilSpec" $ do
  describe "batchBy" $ do
    it "Should batch properly" $ do
      forAll (choose (1 :: Int, 100)) $ \i -> do
        xs :: [[Int]] <- runConduit $ sourceList [1..100] .| batchBy i .| CL.consume
        P.concat xs `shouldBe` [1..100]
        P.filter (/= i) (length <$> P.init xs) `shouldBe` []

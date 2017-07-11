{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.Conduit.CombinatorSpec (spec) where

import           Data.Conduit
import qualified Data.Conduit.List         as CL
import           Data.List.Extra
import           Kafka.Conduit.Combinators
import           Prelude                   as P
import           Test.Hspec
import           Test.QuickCheck

{-# ANN module ("HLint: ignore Redundant do"        :: String) #-}
{-# ANN module ("HLint: ignore Reduce duplication"  :: String) #-}

spec :: Spec
spec = describe "Kafka.Conduit.UtilSpec" $ do
  describe "batchByOrFlush" $ do
    it "Should batch by i properly when Nothing is interspersed between every j elements" $ do
      forAll (choose (1 :: Int, 100)) $ \i ->
        forAll (choose (1 :: Int, 100)) $ \j ->
          testBatchBy batchByOrFlush Just [Nothing] i j
    it "Should batch by i properly when [Nothing, Nothing] is interspersed between every j elements" $ do
      forAll (choose (1 :: Int, 100)) $ \i ->
        forAll (choose (1 :: Int, 100)) $ \j ->
          testBatchBy batchByOrFlush Just [Nothing, Nothing] i j
  describe "batchByOrFlushEither" $ do
    it "Should batch by i properly when Left () is interspersed between every j elements" $ do
      forAll (choose (1 :: Int, 100)) $ \i ->
        forAll (choose (1 :: Int, 100)) $ \j ->
          testBatchBy batchByOrFlushEither Right [Left ()] i j
    it "Should batch by i properly when [Left (), Left ()] is interspersed between every j elements" $ do
      forAll (choose (1 :: Int, 100)) $ \i ->
        forAll (choose (1 :: Int, 100)) $ \j ->
          testBatchBy batchByOrFlushEither Right [Left (), Left ()] i j

testBatchBy :: (Enum a, Num a) => (BatchSize -> ConduitM b [Int] IO ()) -> (a -> b) -> [b] -> Int -> Int -> IO ()
testBatchBy batchFn constr inter i j = do
  let as = intercalate inter (chunksOf j (constr <$> [1..100]))
  xs :: [[Int]] <- runConduit $ CL.sourceList as .| batchFn (BatchSize i) .| CL.consume
  P.concat xs `shouldBe` [1..100]
  P.filter (> i) (length <$> P.init xs) `shouldBe` []
  P.filter (> j) (length <$> P.init xs) `shouldBe` []

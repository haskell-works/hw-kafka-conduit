module Kafka.Conduit.Combinator
  ( batchBy
  , foldYield
  , throwLeft
  ) where

import Control.Exception
import Control.Monad
import Control.Monad.Catch
import Data.Conduit

-- | Throws the left part of a value in a 'MonadThrow' context
throwLeft :: (MonadThrow m, Exception e) => Conduit (Either e i) m i
throwLeft = awaitForever (either throwM yield)

-- | Create a conduit that folds with the function f over its input i with its
-- internal state s and emits outputs [o], then finally emits outputs [o] from
-- the function g applied to the final state s.
foldYield :: Monad m => (i -> s -> (s, [o])) -> (s -> [o]) -> s -> Conduit i m o
foldYield f g s = do
  mi <- await
  case mi of
    Just i -> do
      let (s', os) = f i s
      forM_ os yield
      foldYield f g s'
    Nothing -> forM_ (g s) yield

batchBy :: Monad m => Int -> Conduit a m [a]
batchBy n = foldYield f g (0 :: Int, [])
  where
    f :: a -> (Int, [a]) -> ((Int, [a]), [[a]])
    f a (i, xs) = if i >= n - 1
      then ((0, []), [reverse (a:xs)])
      else ((i + 1, a:xs), [])
    g (_, xs) = [reverse xs]

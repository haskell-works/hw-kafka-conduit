module Kafka.Conduit.Combinators
  ( BatchSize(..)
  , batchByOrFlush
  , batchByOrFlushEither
  , foldYield
  , throwLeft
  , throwLeftSatisfy
  ) where

import Control.Monad       (forM_)
import Control.Monad.Catch (Exception, MonadThrow, throwM)
import Data.Conduit        (ConduitT, await, awaitForever, yield)
import Kafka.Types         (BatchSize (..))

-- | Throws the left part of a value in a 'MonadThrow' context
throwLeft :: (MonadThrow m, Exception e) => ConduitT (Either e i) i m ()
throwLeft = awaitForever (either throwM yield)

-- | Throws the left part of a value in a 'MonadThrow' context if the value
-- satisfies the predicate
throwLeftSatisfy :: (MonadThrow m, Exception e) => (e -> Bool) -> ConduitT (Either e i) (Either e i) m ()
throwLeftSatisfy p = awaitForever awaitHandle
  where awaitHandle (Left e) | p e  = throwM e
        awaitHandle v        = yield v

-- | Create a conduit that folds with the function f over its input i with its
-- internal state s and emits outputs [o], then finally emits outputs [o] from
-- the function g applied to the final state s.
foldYield :: Monad m => (i -> s -> (s, [o])) -> (s -> [o]) -> s -> ConduitT i o m ()
foldYield f g s = do
  mi <- await
  case mi of
    Just i -> do
      let (s', os) = f i s
      forM_ os yield
      foldYield f g s'
    Nothing -> forM_ (g s) yield

batchByOrFlush :: Monad m => BatchSize -> ConduitT (Maybe a) [a] m ()
batchByOrFlush (BatchSize n) = foldYield folder finish (0 :: Int, [])
  where
    folder Nothing  (_, xs) = ((0    ,   []), [reverse    xs ])
    folder (Just a) (i, xs) | (i + 1) >= n  = ((0    ,   []), [reverse (a:xs)])
    folder (Just a) (i, xs) = ((i + 1, a:xs),               [])
    finish (_, xs) = [reverse xs]

batchByOrFlushEither :: Monad m => BatchSize -> ConduitT (Either e a) [a] m ()
batchByOrFlushEither (BatchSize n) = foldYield folder finish (0 :: Int, [])
  where
    folder (Left _)  (_, xs) = ((0    ,    []), [reverse    xs ])
    folder (Right a) (i, xs) | i + 1 >= n = ((0    ,    []), [reverse (a:xs)])
    folder (Right a) (i, xs) = ((i + 1, a:xs ),               [])
    finish (_, xs) = [reverse xs]

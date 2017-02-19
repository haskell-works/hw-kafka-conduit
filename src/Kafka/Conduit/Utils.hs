module Kafka.Conduit.Utils where

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

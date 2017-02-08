module Kafka.Conduit.Utils
where

import Control.Exception
import Control.Monad.Catch
import Data.Conduit

throwLeft :: (MonadThrow m, Exception e) => Conduit (Either e i) m i
throwLeft = awaitForever (either throwM yield)

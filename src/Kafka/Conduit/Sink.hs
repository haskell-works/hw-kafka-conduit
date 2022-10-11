module Kafka.Conduit.Sink
( module X
, kafkaSink, kafkaSinkAutoClose, kafkaSinkNoClose
, commitOffsetsSink, flushThenCommitSink
) where

import           Control.Monad                (void)
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Resource
import           Data.Conduit
import qualified Data.Conduit.List            as L
import           Kafka.Consumer

import Kafka.Conduit.Combinators as X
import Kafka.Consumer            as X (KafkaConsumer)
import Kafka.Producer            as X


-- | Creates a Sink for a given `KafkaProducer`.
-- The producer will be closed when the Sink is closed.
kafkaSinkAutoClose :: MonadResource m
                   => KafkaProducer
                   -> ConduitT ProducerRecord Void m (Maybe KafkaError)
kafkaSinkAutoClose prod =
  bracketP (return prod) (void . closeProducer) runHandler
  where
    runHandler p' = do
      mbMsg <- await
      case mbMsg of
        Nothing -> return Nothing
        Just msg -> do
          res <- produceMessage p' msg
          case res of
            Nothing  -> runHandler p'
            Just err -> return (Just err)

-- | Creates a Sink for a given `KafkaProducer`.
-- The producer will NOT be closed automatically.
kafkaSinkNoClose :: MonadIO m
                 => KafkaProducer
                 -> ConduitT ProducerRecord Void m (Maybe KafkaError)
kafkaSinkNoClose prod = go
  where
    go = do
      mbMsg <- await
      case mbMsg of
        Nothing -> return Nothing
        Just msg -> do
          res <- produceMessage prod msg
          case res of
            Nothing  -> go
            Just err -> return (Just err)

-- | Creates a kafka producer for given properties and returns a Sink.
--
-- This method of creating a Sink represents a simple case
-- and does not provide access to `KafkaProducer`. For more complex scenarious
-- 'kafkaSinkAutoClose' or 'kafkaSinkNoClose' can be used.
kafkaSink :: MonadResource m
          => ProducerProperties
          -> ConduitT ProducerRecord Void m (Maybe KafkaError)
kafkaSink props =
  bracketP mkProducer clProducer runHandler
  where
    mkProducer = newProducer props

    clProducer (Left _)     = return ()
    clProducer (Right prod) = void $ closeProducer prod

    runHandler (Left err) = return (Just err)
    runHandler (Right prod) = do
      mbMsg <- await
      case mbMsg of
        Nothing -> return Nothing
        Just msg -> do
          res <- produceMessage prod msg
          runHandler $ maybe (Right prod) Left res

-- | Ignores incoming messages and commits offsets. Commit errors are ignored.
-- This functionality should not exist as a Sink and will be removed in future versions.
-- Consider having an effect instead:
--
-- > mapMC (\_ -> commitAllOffsets OffsetCommit consumer)
{-# DEPRECATED commitOffsetsSink "Conceptually wrong thing to do. Does not require library support. Consider calling 'commitAllOffsets' when appropriate." #-}
commitOffsetsSink :: MonadIO m => KafkaConsumer -> ConduitT i Void m ()
commitOffsetsSink = flip commitOffsetsSink' (const $ pure ())

-- | Ignores incoming messages and commits offsets. Commit errors are handled with 'handleError' effect.
-- This functionality should not exist as a Sink and will be removed in future versions.
-- Consider having an effect instead:
--
-- > mapMC (\_ -> commitAllOffsets OffsetCommit consumer >>= handleError)
{-# DEPRECATED commitOffsetsSink' "Conceptually wrong thing to do. Does not require library support. Consider calling 'commitAllOffsets' when appropriate." #-}
commitOffsetsSink':: MonadIO m => KafkaConsumer -> (KafkaError -> m ()) -> ConduitT i Void m ()
commitOffsetsSink' consumer handleError = L.mapM_ $ \_ -> do
  res <- commitAllOffsets OffsetCommit consumer
  case res of
    Nothing  -> pure ()
    Just err -> handleError err

-- | Ignores incoming messages and commits offsets, but makes sure that 'producer' has an empty outgoing queue.
-- Commit errors are ignored.
-- This functionality should not exist as a Sink and will be removed in future versions.
-- Consider having an effect instead:
--
-- > mapMC (\_ -> flushProducer producer >>= commitAllOffsets OffsetCommit consumer)
{-# DEPRECATED flushThenCommitSink "Conceptually wrong thing to do. Does not require library support. Consider calling 'flushProducer >>= commitAllOffsets' when appropriate." #-}
flushThenCommitSink :: MonadIO m => KafkaConsumer -> KafkaProducer -> ConduitT i Void m ()
flushThenCommitSink consumer producer = flushThenCommitSink' consumer producer (const $ pure ())

-- | Ignores incoming messages and commits offsets, but makes sure that 'producer' has an empty outgoing queue.
-- Commit errors are handled with 'handleError' effect.
-- This functionality should not exist as a Sink and will be removed in future versions.
-- Consider having an effect instead:
--
-- > mapMC (\_ -> flushProducer producer >>= commitAllOffsets OffsetCommit consumer >>= handleError)
{-# DEPRECATED flushThenCommitSink' "Conceptually wrong thing to do. Does not require library support. Consider calling 'flushProducer >>= commitAllOffsets' when appropriate." #-}
flushThenCommitSink' :: MonadIO m => KafkaConsumer -> KafkaProducer -> (KafkaError -> m ()) -> ConduitT i Void m ()
flushThenCommitSink' consumer producer handleError = L.mapM_ $ \_ -> do
  flushProducer producer
  res <- commitAllOffsets OffsetCommit consumer
  case res of
    Nothing  -> pure ()
    Just err -> handleError err

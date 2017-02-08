module Kafka.Conduit.Sink
( module X
, kafkaSink, kafkaSinkAutoClose, kafkaSinkNoClose
) where
--

import Control.Monad.IO.Class
import Control.Monad (void)
import Control.Monad.Trans.Resource
import Data.Conduit
import Kafka.Producer as X
import Kafka.Conduit.Utils as X

kafkaSinkAutoClose :: MonadResource m
                   => KafkaProducer
                   -> Sink ProducerRecord m (Maybe KafkaError)
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
            Nothing -> runHandler p'
            Just err -> return (Just err)

kafkaSinkNoClose :: MonadIO m
                 => KafkaProducer
                 -> Sink ProducerRecord m (Maybe KafkaError)
kafkaSinkNoClose prod = go
  where
    go = do
      mbMsg <- await
      case mbMsg of
        Nothing -> return Nothing
        Just msg -> do
          res <- produceMessage prod msg
          case res of
            Nothing -> go
            Just err -> return (Just err)

kafkaSink :: MonadResource m
          => ProducerProperties
          -> Sink ProducerRecord m (Maybe KafkaError)
kafkaSink props =
  bracketP mkProducer clProducer runHandler
  where
    mkProducer = newProducer props

    clProducer (Left _) = return ()
    clProducer (Right prod) = void $ closeProducer prod

    runHandler (Left err) = return (Just err)
    runHandler (Right prod) = do
      mbMsg <- await
      case mbMsg of
        Nothing -> return Nothing
        Just msg -> do
          res <- produceMessage prod msg
          runHandler $ maybe (Right prod) Left res

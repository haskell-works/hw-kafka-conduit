{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Consumer
( module X
, kafkaSource, kafkaSource'
, isFatal
, ConsumerRecord(..), ConsumerGroupId(..)
, KafkaError(..), RdKafkaRespErrT(..)
) where

import Control.Monad.IO.Class
import Control.Monad (void)
import qualified Data.ByteString as BS
import Data.Conduit
import Kafka
import Kafka.Consumer.ConsumerProperties as X
import Kafka.Consumer.Subscription as X
import Kafka.Consumer hiding (newConsumer, closeConsumer, pollMessage)
import qualified Kafka.Consumer as K
import Control.Monad.Trans.Resource

kafkaSource' :: MonadResource m
             => KafkaConsumer
             -> Timeout
             -> Source m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSource' c ts =
  bracketP mkConsumer clConsumer runHandler
  where
    mkConsumer = return c
    clConsumer c' = void $ liftIO (K.closeConsumer c')
    runHandler c' = do
      msg <- liftIO $ K.pollMessage c' ts
      -- stop at some certain cases because it is not goind to be better with time
      case msg of
        Left err | isFatal err -> void $ yield (Left err)
        _ -> yield msg >> runHandler c'

-- | Conduit source for kafka consumer.
kafkaSource :: MonadResource m
            => ConsumerProperties
            -> Subscription
            -> Timeout
            -> Source m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSource props sub ts =
  bracketP mkConsumer clConsumer runHandler
  where
      mkConsumer = liftIO $ K.newConsumer props sub

      clConsumer (Left _) = return ()
      clConsumer (Right c) = void $ liftIO (K.closeConsumer c)

      runHandler (Left err) = void $ yield (Left err)
      runHandler (Right c) = do
        msg <- liftIO $ K.pollMessage c ts
        -- stop at some certain cases because it is not goind to be better with time
        case msg of
          Left err | isFatal err -> void $ yield (Left err)
          _ -> yield msg >> runHandler (Right c)

-- | Checks if the error is fatal in a way that it doesn't make sense to retry.
isFatal :: KafkaError -> Bool
isFatal e = case e of
  KafkaUnknownConfigurationKey _   -> True
  KafkaInvalidConfigurationValue _ -> True
  KakfaBadConfiguration            -> True
  KafkaBadSpecification _          -> True

  -- More of them? Less of them?
  KafkaResponseError RdKafkaRespErrDestroy                    -> True
  KafkaResponseError RdKafkaRespErrFail                       -> True
  KafkaResponseError RdKafkaRespErrInvalidArg                 -> True
  KafkaResponseError RdKafkaRespErrSsl                        -> True
  KafkaResponseError RdKafkaRespErrUnknownProtocol            -> True
  KafkaResponseError RdKafkaRespErrNotImplemented             -> True
  KafkaResponseError RdKafkaRespErrAuthentication             -> True
  KafkaResponseError RdKafkaRespErrInconsistentGroupProtocol  -> True
  KafkaResponseError RdKafkaRespErrTopicAuthorizationFailed   -> True
  KafkaResponseError RdKafkaRespErrGroupAuthorizationFailed   -> True
  KafkaResponseError RdKafkaRespErrClusterAuthorizationFailed -> True
  KafkaResponseError RdKafkaRespErrUnsupportedSaslMechanism   -> True
  KafkaResponseError RdKafkaRespErrIllegalSaslState           -> True
  KafkaResponseError RdKafkaRespErrUnsupportedVersion         -> True

  _ -> False

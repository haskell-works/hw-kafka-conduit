{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Consumer
( kafkaSource
, newConsumer, closeConsumer
, pollMessage
, subscribe
, newConsumerConf, newConsumerTopicConf
, ReceivedMessage(..), ConsumerGroupId(..)
, KafkaError(..), RdKafkaRespErrT(..)
) where

import Control.Monad.IO.Class
import Control.Monad (void)
import Data.Conduit
import Kafka
import Kafka.Consumer (ReceivedMessage, ConsumerGroupId, RdKafkaRespErrT(..))
import qualified Kafka.Consumer as K
import Control.Monad.Trans.Resource

-- | Creates a new kafka configuration for a consumer with a specified 'ConsumerGroupId'.
newConsumerConf :: MonadIO m
                => ConsumerGroupId                  -- ^ Consumer group id (a @group.id@ property of a kafka consumer)
                -> KafkaProps                       -- ^ Extra kafka consumer parameters (see kafka documentation)
                -> m KafkaConf                      -- ^ Kafka configuration which can be altered before it is used in 'newConsumer'
newConsumerConf g p = liftIO $ K.newConsumerConf g p

-- | Creates a new topic configuration from the specified properties
newConsumerTopicConf :: MonadIO m => TopicProps -> m TopicConf
newConsumerTopicConf = liftIO . K.newConsumerTopicConf

-- | Creates a new kafka consumer. Note: it must be properly closed after it is not needed.
newConsumer :: MonadIO m
            => KafkaConf                            -- ^ Consumer config
            -> TopicConf                            -- ^ Topic config that is going to be used for every topic consumed by the consumer
            -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
            -> m Kafka
newConsumer kc tc bs =
  liftIO $ K.setDefaultTopicConf kc tc >> K.newConsumer bs kc

-- | Closes the specified kafka consumer.
closeConsumer :: MonadIO m => Kafka -> m (Maybe KafkaError)
closeConsumer = liftIO . K.closeConsumer

-- | Subscribes to a list of topics
subscribe :: MonadIO m => Kafka -> [TopicName] -> m (Maybe KafkaError)
subscribe k ts = liftIO $ K.subscribe k ts
{-# INLINE subscribe #-}

-- | Polls one message from the consumer.
pollMessage :: MonadIO m => Kafka -> Timeout -> m (Either KafkaError ReceivedMessage)
pollMessage k t = liftIO $ K.pollMessage k t
{-# INLINE pollMessage #-}

-- | Conduit source for kafka consumer.
kafkaSource :: MonadResource m
            => KafkaConf                            -- ^ Consumer config
            -> TopicConf                            -- ^ Topic config that is going to be used for every topic consumed by the consumer
            -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
            -> Timeout                              -- ^ Poll timeout
            -> [TopicName]                          -- ^ List of topics to be consumed
            -> Source m (Either KafkaError ReceivedMessage)
kafkaSource kc tc bs t ts =
  bracketP mkConsumer clConsumer runHandler
  where
      mkConsumer = do
          kafka  <- newConsumer kc tc bs
          sres   <- subscribe kafka ts
          return (kafka, sres)

      clConsumer (kafka, _) = void $ closeConsumer kafka

      runHandler (_, Just err) = void $ yield (Left err)
      runHandler (kafka, Nothing) = do
        msg <- pollMessage kafka t
        -- stop at some certain cases because it is not goind to be better with time
        case msg of
          Left err | isFatal err -> void $ yield (Left err)
          _ -> yield msg >> runHandler (kafka, Nothing)

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

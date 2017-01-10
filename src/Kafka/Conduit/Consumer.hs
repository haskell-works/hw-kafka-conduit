{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Consumer
( kafkaSource
, kafkaSource'
, newConsumer
, subscribe
, newConsumerConf, newConsumerTopicConf
, ReceivedMessage(..), ConsumerGroupId(..)
) where

import Control.Monad.IO.Class
import Control.Monad (void)
import Data.Conduit
import Kafka
import Kafka.Consumer (ReceivedMessage, ConsumerGroupId)
import qualified Kafka.Consumer as K
import Control.Monad.Trans.Resource

-- | Creates a new kafka configuration for a consumer with a specified 'ConsumerGroupId'.
newConsumerConf :: MonadIO m
                => ConsumerGroupId                  -- ^ Consumer group id (a @group.id@ property of a kafka consumer)
                -> KafkaProps                       -- ^ Extra kafka consumer parameters (see kafka documentation)
                -> m KafkaConf                      -- ^ Kafka configuration which can be altered before it is used in 'newConsumer'
newConsumerConf g p = liftIO $ K.newConsumerConf g p

newConsumerTopicConf :: MonadIO m => TopicProps -> m TopicConf
newConsumerTopicConf = liftIO . K.newConsumerTopicConf

newConsumer :: (MonadResource m)
            => KafkaConf                            -- ^ Consumer config
            -> TopicConf                            -- ^ Topic config that is going to be used for every topic consumed by the consumer
            -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
            -> m Kafka
newConsumer kc tc bs =
  let create = K.setDefaultTopicConf kc tc >> K.newConsumer bs kc
      close  = void <$> K.closeConsumer
   in snd <$> allocate create close

subscribe :: MonadIO m => Kafka -> [TopicName] -> m (Maybe KafkaError)
subscribe k ts = liftIO $ subscribe k ts
{-# INLINE subscribe #-}

pollMessage :: MonadIO m => Kafka -> Timeout -> m (Either KafkaError ReceivedMessage)
pollMessage k t = liftIO $ K.pollMessage k t
{-# INLINE pollMessage #-}

kafkaSource :: MonadResource m
            => Kafka
            -> Timeout
            -> Source m (Either KafkaError ReceivedMessage)
kafkaSource kafka timeout = go
  where
    go = yieldM (pollMessage kafka timeout) >> go


-- REDO using newConsumer? remove? Don't like these Left/Right cleanup magic
kafkaSource' :: MonadResource m
            => KafkaConf                            -- ^ Consumer config
            -> TopicConf                            -- ^ Topic config that is going to be used for every topic consumed by the consumer
            -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
            -> [TopicName]                          -- ^ List of topics to be consumed
            -> Timeout
            -> Source m (Either KafkaError ReceivedMessage)
kafkaSource' kc tc bs ts t =
    bracketP mkConsumer clConsumer runHandler
    where
        mkConsumer = do
            K.setDefaultTopicConf kc tc
            kafka  <- K.newConsumer bs kc
            subRes <- K.subscribe kafka ts
            return $ maybe (Right kafka) (Left . (, kafka)) subRes

        clConsumer (Left (_, kafka)) = void $ K.closeConsumer kafka
        clConsumer (Right kafka) = void $ K.closeConsumer kafka

        runHandler (Left (err, _)) = return ()
        runHandler (Right kafka) = do
          liftIO $ print "Let's poll"
          msg <- liftIO $ K.pollMessage kafka t
          yield msg
          runHandler (Right kafka)

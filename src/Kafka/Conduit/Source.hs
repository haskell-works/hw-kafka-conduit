{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Source
( module X
, newConsumer, closeConsumer
, kafkaSource, kafkaSourceNoClose, kafkaSourceAutoClose
, commitAllOffsets, commitOffsetMessage
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
import Kafka.Consumer hiding (newConsumer, closeConsumer, pollMessage, commitAllOffsets, commitOffsetMessage)
import qualified Kafka.Consumer as K
import Control.Monad.Trans.Resource

newConsumer :: MonadIO m
            => ConsumerProperties
            -> Subscription
            -> m (Either KafkaError KafkaConsumer)
newConsumer p s = liftIO $ K.newConsumer p s

closeConsumer :: MonadIO m
              => KafkaConsumer
              -> m (Maybe KafkaError)
closeConsumer = liftIO . K.closeConsumer

commitAllOffsets :: MonadIO m
                 => OffsetCommit
                 -> KafkaConsumer
                 -> m (Maybe KafkaError)
commitAllOffsets o c = liftIO $ K.commitAllOffsets o c

commitOffsetMessage :: MonadIO m
                    => OffsetCommit
                    -> KafkaConsumer
                    -> ConsumerRecord k v
                    -> m (Maybe KafkaError)
commitOffsetMessage o c r = liftIO $ K.commitOffsetMessage o c r

kafkaSourceNoClose :: MonadIO m
                   => KafkaConsumer
                   -> Timeout
                   -> Source m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSourceNoClose c t = go
  where
    go = do
      msg <- liftIO $ K.pollMessage c t
      -- stop at some certain cases because it is not goind to be better with time
      case msg of
        Left err | isFatal err -> void $ yield (Left err)
        _ -> yield msg >> go

kafkaSourceAutoClose :: MonadResource m
                     => KafkaConsumer
                     -> Timeout
                     -> Source m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSourceAutoClose c ts =
  bracketP mkConsumer clConsumer runHandler
  where
    mkConsumer = return c
    clConsumer c' = void $ closeConsumer c'
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
      mkConsumer = newConsumer props sub

      clConsumer (Left _) = return ()
      clConsumer (Right c) = void $ closeConsumer c

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

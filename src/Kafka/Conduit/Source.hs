{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Source
( module X
, kafkaSource, kafkaSourceNoClose, kafkaSourceAutoClose
, isFatal
, skipNonFatal
) where

import Control.Monad.IO.Class
import Control.Monad (void)
import Control.Monad.Trans.Resource
import qualified Data.ByteString as BS
import Data.Conduit
import qualified Data.Conduit.List as L
import Kafka.Consumer as X
import Kafka.Conduit.Utils as X

kafkaSourceNoClose :: MonadIO m
                   => KafkaConsumer
                   -> Timeout
                   -> Source m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSourceNoClose c t = go
  where
    go = do
      msg <- pollMessage c t
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
      msg <- pollMessage c' ts
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
        msg <- pollMessage c ts
        -- stop at some certain cases because it is not goind to be better with time
        case msg of
          Left err | isFatal err -> void $ yield (Left err)
          _ -> yield msg >> runHandler (Right c)

skipNonFatal :: Monad m => Conduit (Either KafkaError b) m (Either KafkaError b)
skipNonFatal = L.filter (either isFatal (const True))

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

{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Source
( module X
, kafkaSource, kafkaSourceNoClose, kafkaSourceAutoClose
, mapRecordKey, mapRecordValue, mapRecordKV
, traverseRecordKey, traverseRecordValue, traverseRecordKV
, traverseRecordKeyM, traverseRecordValueM, traverseRecordKVM
, isFatal, isPollTimeout, isPartitionEOF
, skipNonFatal, nonFatalOr
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

---------------------- Useful ConsumerRecord combinators -----------------------
mapRecordKey :: Monad m => (k -> k') -> Conduit (ConsumerRecord k v) m (ConsumerRecord k' v)
mapRecordKey f = L.map (crMapKey f)
{-# INLINE mapRecordKey #-}

mapRecordValue :: Monad m => (v -> v') -> Conduit (ConsumerRecord k v) m (ConsumerRecord k v')
mapRecordValue f = L.map (crMapValue f)
{-# INLINE mapRecordValue #-}

mapRecordKV :: Monad m => (k -> k') -> (v -> v') -> Conduit (ConsumerRecord k v) m (ConsumerRecord k' v')
mapRecordKV f g = L.map (crMapKV f g)
{-# INLINE mapRecordKV #-}

traverseRecordKey :: (Functor t, Monad m) => (k -> t k') -> Conduit (ConsumerRecord k v) m (t (ConsumerRecord k' v))
traverseRecordKey f = L.map (crTraverseKey f)
{-# INLINE traverseRecordKey #-}

traverseRecordValue :: (Functor t, Monad m) => (v -> t v') -> Conduit (ConsumerRecord k v) m (t (ConsumerRecord k v'))
traverseRecordValue f = L.map (crTraverseValue f)
{-# INLINE traverseRecordValue #-}

traverseRecordKV :: (Applicative t, Monad m) => (k -> t k') -> (v -> t v') -> Conduit (ConsumerRecord k v) m (t (ConsumerRecord k' v'))
traverseRecordKV f g = L.map (crTraverseKV f g)
{-# INLINE traverseRecordKV #-}

traverseRecordKeyM :: (Functor t, Monad m) => (k -> m (t k')) -> Conduit (ConsumerRecord k v) m (t (ConsumerRecord k' v))
traverseRecordKeyM f = L.mapM (crTraverseKeyM f)
{-# INLINE traverseRecordKeyM #-}

traverseRecordValueM :: (Functor t, Monad m) => (v -> m (t v')) -> Conduit (ConsumerRecord k v) m (t (ConsumerRecord k v'))
traverseRecordValueM f = L.mapM (crTraverseValueM f)
{-# INLINE traverseRecordValueM #-}

traverseRecordKVM :: (Applicative t, Monad m) => (k -> m (t k')) -> (v -> m (t v')) -> Conduit (ConsumerRecord k v) m (t (ConsumerRecord k' v'))
traverseRecordKVM f g = L.mapM (crTraverseKVM f g)
{-# INLINE traverseRecordKVM #-}

--------------------------------------------------------------------------------

skipNonFatal :: Monad m => Conduit (Either KafkaError b) m (Either KafkaError b)
skipNonFatal = L.filter (either isFatal (const True))
{-# INLINE skipNonFatal #-}

isPollTimeout :: KafkaError -> Bool
isPollTimeout e = KafkaResponseError RdKafkaRespErrTimedOut == e
{-# INLINE isPollTimeout #-}

isPartitionEOF :: KafkaError -> Bool
isPartitionEOF e = KafkaResponseError RdKafkaRespErrPartitionEof == e
{-# INLINE isPartitionEOF #-}

nonFatalOr :: Monad m => [KafkaError -> Bool] -> Conduit (Either KafkaError b) m (Either KafkaError b)
nonFatalOr fs =
  let fun e = or $ (\f -> f e) <$> (isFatal : fs)
   in L.filter (either fun (const True))
{-# INLINE nonFatalOr #-}

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

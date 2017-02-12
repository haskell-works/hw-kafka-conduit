{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Source
( module X
, kafkaSource, kafkaSourceNoClose, kafkaSourceAutoClose
, mapFirst, mapF, bimapF
, sequenceValueFirst, sequenceValue, bisequenceValue
, traverseValueFirst, traverseValue, bitraverseValue
, traverseValueFirstM, traverseValueM, bitraverseValueM
, isFatal, isPollTimeout, isPartitionEOF
, skipNonFatal, nonFatalOr
) where

import Data.Bifunctor
import Data.Bitraversable
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
mapFirst :: (Bifunctor t, Monad m) => (k -> k') -> Conduit (t k v) m (t k' v)
mapFirst f = L.map (first f)
{-# INLINE mapFirst #-}

mapF :: (Functor t, Monad m) => (v -> v') -> Conduit (t v) m (t v')
mapF f = L.map (fmap f)
{-# INLINE mapF #-}

bimapF :: (Bifunctor t, Monad m) => (k -> k') -> (v -> v') -> Conduit (t k v) m (t k' v')
bimapF f g = L.map (bimap f g)
{-# INLINE bimapF #-}

sequenceValueFirst :: (Bitraversable t, Applicative f, Monad m) => Conduit (t (f k) v) m (f (t k v))
sequenceValueFirst = L.map sequenceFirst
{-# INLINE sequenceValueFirst #-}

sequenceValue :: (Traversable t, Applicative f, Monad m) => Conduit (t (f v)) m (f (t v))
sequenceValue = L.map sequenceA
{-# INLINE sequenceValue #-}

bisequenceValue :: (Bitraversable t, Applicative f, Monad m) => Conduit (t (f k) (f v)) m (f (t k v))
bisequenceValue = L.map bisequenceA
{-# INLINE bisequenceValue #-}

traverseValueFirst :: (Bitraversable t, Applicative f, Monad m) => (k -> f k') -> Conduit (t k v) m (f (t k' v))
traverseValueFirst f = L.map (traverseFirst f)
{-# INLINE traverseValueFirst #-}

traverseValue :: (Traversable t, Applicative f, Monad m) => (v -> f v') -> Conduit (t v) m (f (t v'))
traverseValue f = L.map (traverse f)
{-# INLINE traverseValue #-}

bitraverseValue :: (Bitraversable t, Applicative f, Monad m) => (k -> f k') -> (v -> f v') -> Conduit (t k v) m (f (t k' v'))
bitraverseValue f g = L.map (bitraverse f g)
{-# INLINE bitraverseValue #-}

traverseValueFirstM :: (Bitraversable t, Applicative f, Monad m) => (k -> m (f k')) -> Conduit (t k v) m (f (t k' v))
traverseValueFirstM f = L.mapM (traverseFirstM f)
{-# INLINE traverseValueFirstM #-}

traverseValueM :: (Traversable t, Applicative f, Monad m) => (v -> m (f v')) -> Conduit (t v) m (f (t v'))
traverseValueM f = L.mapM (traverseM f)
{-# INLINE traverseValueM #-}

bitraverseValueM :: (Bitraversable t, Applicative f, Monad m) => (k -> m (f k')) -> (v -> m (f v')) -> Conduit (t k v) m (f (t k' v'))
bitraverseValueM f g = L.mapM (bitraverseM f g)
{-# INLINE bitraverseValueM #-}

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

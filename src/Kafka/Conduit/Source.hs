{-# LANGUAGE TupleSections#-}
module Kafka.Conduit.Source
( -- ** Source
  kafkaSource, kafkaSourceNoClose, kafkaSourceAutoClose

  -- ** Error handling
, isFatal, isPollTimeout, isPartitionEOF
, skipNonFatal, skipNonFatalExcept
  -- ** Utility combinators
, mapFirst, mapValue, bimapValue
, sequenceValueFirst, sequenceValue, bisequenceValue
, traverseValueFirst, traverseValue, bitraverseValue
, traverseValueFirstM, traverseValueM, bitraverseValueM
, module X
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

-- | Create a `Source` for a given `KafkaConsumer`.
-- The consumer will NOT be closed automatically when the `Source` is closed.
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

-- | Create a `Source` for a given `KafkaConsumer`.
-- The consumer will be closed automatically when the `Source` is closed.
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

-- | Creates a kafka producer for given properties and returns a `Source`.
--
-- This method of creating a `Source` represents a simple case
-- and does not provide access to `KafkaProducer`. For more complex scenarious
-- 'kafkaSinkNoClose' or 'kafkaSinkAutoClose' can be used.
kafkaSource :: MonadResource m
            => ConsumerProperties
            -> Subscription
            -> Timeout              -- ^ Poll timeout
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

------------------------------- Utitlity functions

-- | Maps over the first element of a value
--
-- > mapFirst f = L.map (first f)
mapFirst :: (Bifunctor t, Monad m) => (k -> k') -> Conduit (t k v) m (t k' v)
mapFirst f = L.map (first f)
{-# INLINE mapFirst #-}

-- | Maps over a value
--
-- > mapValue f = L.map (fmap f)
mapValue :: (Functor t, Monad m) => (v -> v') -> Conduit (t v) m (t v')
mapValue f = L.map (fmap f)
{-# INLINE mapValue #-}

-- | Bimaps (maps over both the first and the second element) over a value
--
-- > bimapValue f g = L.map (bimap f g)
bimapValue :: (Bifunctor t, Monad m) => (k -> k') -> (v -> v') -> Conduit (t k v) m (t k' v')
bimapValue f g = L.map (bimap f g)
{-# INLINE bimapValue #-}

-- | Sequences the first element of a value
--
-- > sequenceValueFirst = L.map sequenceFirst
sequenceValueFirst :: (Bitraversable t, Applicative f, Monad m) => Conduit (t (f k) v) m (f (t k v))
sequenceValueFirst = L.map sequenceFirst
{-# INLINE sequenceValueFirst #-}

-- | Sequences the value
--
-- > sequenceValue = L.map sequenceA
sequenceValue :: (Traversable t, Applicative f, Monad m) => Conduit (t (f v)) m (f (t v))
sequenceValue = L.map sequenceA
{-# INLINE sequenceValue #-}

-- | Sequences both the first and the second element of a value (bisequences the value)
--
-- > bisequenceValue = L.map bisequenceA
bisequenceValue :: (Bitraversable t, Applicative f, Monad m) => Conduit (t (f k) (f v)) m (f (t k v))
bisequenceValue = L.map bisequenceA
{-# INLINE bisequenceValue #-}

-- | Traverses over the first element of a value
--
-- > traverseValueFirst f = L.map (traverseFirst f)
traverseValueFirst :: (Bitraversable t, Applicative f, Monad m) => (k -> f k') -> Conduit (t k v) m (f (t k' v))
traverseValueFirst f = L.map (traverseFirst f)
{-# INLINE traverseValueFirst #-}

-- | Traverses over the value
--
-- > L.map (traverse f)
traverseValue :: (Traversable t, Applicative f, Monad m) => (v -> f v') -> Conduit (t v) m (f (t v'))
traverseValue f = L.map (traverse f)
{-# INLINE traverseValue #-}

-- | Traverses over both the first and the second elements of a value (bitraverses over a value)
--
-- > bitraverseValue f g = L.map (bitraverse f g)
bitraverseValue :: (Bitraversable t, Applicative f, Monad m) => (k -> f k') -> (v -> f v') -> Conduit (t k v) m (f (t k' v'))
bitraverseValue f g = L.map (bitraverse f g)
{-# INLINE bitraverseValue #-}

-- | Monadically traverses over the first element of a value
--
-- > traverseValueFirstM f = L.mapM (traverseFirstM f)
traverseValueFirstM :: (Bitraversable t, Applicative f, Monad m) => (k -> m (f k')) -> Conduit (t k v) m (f (t k' v))
traverseValueFirstM f = L.mapM (traverseFirstM f)
{-# INLINE traverseValueFirstM #-}

-- | Monadically traverses over a value
--
-- > traverseValueM f = L.mapM (traverseM f)
traverseValueM :: (Traversable t, Applicative f, Monad m) => (v -> m (f v')) -> Conduit (t v) m (f (t v'))
traverseValueM f = L.mapM (traverseM f)
{-# INLINE traverseValueM #-}

-- | Monadically traverses over both the first and the second elements of a value
-- (monadically bitraverses over a value)
--
-- > bitraverseValueM f g = L.mapM (bitraverseM f g)
bitraverseValueM :: (Bitraversable t, Applicative f, Monad m) => (k -> m (f k')) -> (v -> m (f v')) -> Conduit (t k v) m (f (t k' v'))
bitraverseValueM f g = L.mapM (bitraverseM f g)
{-# INLINE bitraverseValueM #-}

--------------------------------------------------------------------------------

-- | Filters out non-fatal errors (see 'isFatal') and only allows fatal errors
-- to be propagated downstream.
skipNonFatal :: Monad m => Conduit (Either KafkaError b) m (Either KafkaError b)
skipNonFatal = L.filter (either isFatal (const True))
{-# INLINE skipNonFatal #-}

-- | Checks if the provided error is a timeout error ('KafkaResponseError' 'RdKafkaRespErrTimedOut').
--
-- Timeout errors are not fatal and occure, for example, in cases when 'pollMessage'
-- cannot return return a message after the specified poll timeout (no more messages in a topic).
--
-- Often this error can be ignored, however sometimes it can be useful to know that
-- there was no more messages in a topic and "'KafkaResponseError' 'RdKafkaRespErrTimedOut'"
-- can be a good indicator of that.
isPollTimeout :: KafkaError -> Bool
isPollTimeout e = KafkaResponseError RdKafkaRespErrTimedOut == e
{-# INLINE isPollTimeout #-}

-- | Checks if the provided error is an indicator of reaching the end of a partition
-- ('KafkaResponseError' 'RdKafkaRespErrPartitionEof').
--
-- @PartitionEOF@ errors are not fatal and occure every time a consumer reaches the end
-- of a partition.
--
-- Often this error can be ignored, however sometimes it can be useful to know that
-- a partition has exhausted and "'KafkaResponseError' 'RdKafkaRespErrPartitionEof'"
-- can be a good indicator of that.
isPartitionEOF :: KafkaError -> Bool
isPartitionEOF e = KafkaResponseError RdKafkaRespErrPartitionEof == e
{-# INLINE isPartitionEOF #-}

-- | Filters out non-fatal errors and provides the ability to control which
-- non-fatal errors will still be propagated downstream.
--
-- Example:
-- > skipNonFatalExcept [isPollTimeout, isPartitionEOF]
-- The instruction above skips all the non-fatal errors except for
-- "'KafkaResponseError' 'RdKafkaRespErrTimedOut'" and "'KafkaResponseError' 'RdKafkaRespErrPartitionEof'".
--
-- This function does not allow filtering out fatal errors.
skipNonFatalExcept :: Monad m => [KafkaError -> Bool] -> Conduit (Either KafkaError b) m (Either KafkaError b)
skipNonFatalExcept fs =
  let fun e = or $ (\f -> f e) <$> (isFatal : fs)
   in L.filter (either fun (const True))
{-# INLINE skipNonFatalExcept #-}

-- | Checks if the error is fatal in a way that it doesn't make sense to retry after
-- or is unsafe to ignore.
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

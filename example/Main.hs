{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Monad (forM_)
import Control.Monad.Trans.Resource
import Data.Monoid ((<>))
import Data.ByteString (ByteString)
import Kafka.Conduit.Source
import Kafka.Conduit.Sink
import Data.Conduit
import qualified Data.Conduit.List as L

-- Topic to write to and read from
testTopic :: TopicName
testTopic = TopicName "kafka-client-conduit-example-topic"

-- Global consumer properties
consumerProps :: ConsumerProperties
consumerProps = consumerBrokersList [BrokerAddress "localhost:9092"]
             <> groupId (ConsumerGroupId "consumer_example_group")
             <> noAutoCommit

-- Subscription to topics
consumerSub :: Subscription
consumerSub = topics [testTopic]
           <> offsetReset Earliest

-- Global producer properties
producerProps :: ProducerProperties
producerProps = producerBrokersList [BrokerAddress "localhost:9092"]

main :: IO ()
main = do
  putStrLn "Running sink..."
  producerRes <- runConduitRes outputStream
  forM_ producerRes print
  putStrLn "Running source..."
  msgs <- runConduitRes $ inputStream .| L.take 10
  forM_ msgs print
  putStrLn "Ok."

inputStream =
  kafkaSource consumerProps consumerSub (Timeout 1000)

outputStream =
  L.sourceList ["c-one", "c-two", "c-three"]
  .| L.map (ProducerRecord testTopic UnassignedPartition)
  .| kafkaSink producerProps

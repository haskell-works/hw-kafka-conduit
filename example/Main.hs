{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Monad        (forM_)
import           Data.Conduit
import qualified Data.Conduit.List    as L
import           Data.Monoid          ((<>))
import           Kafka.Conduit.Sink   as KSnk
import           Kafka.Conduit.Source as KSrc

kafkaBroker :: BrokerAddress
kafkaBroker = BrokerAddress "localhost:9092"

-- Topic to write to and read from
testTopic :: TopicName
testTopic = TopicName "kafka-client-conduit-example-topic"

-- Global consumer properties
consumerProps :: ConsumerProperties
consumerProps = KSrc.brokersList [kafkaBroker]
             <> groupId (ConsumerGroupId "consumer_conduit_example_group")
             <> noAutoCommit

-- Subscription to topics
consumerSub :: Subscription
consumerSub = topics [testTopic]
           <> offsetReset Earliest

-- Global producer properties
producerProps :: ProducerProperties
producerProps = KSnk.brokersList [kafkaBroker]

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
  .| L.map ((ProducerRecord testTopic UnassignedPartition Nothing) . Just)
  .| kafkaSink producerProps

module Kafka.Conduit
where

-- import           Data.Conduit
-- import qualified Data.Conduit.List as CL
-- import           Kafka
-- import           Kafka.Producer

-- kafkaSink :: Kafka -> TopicName -> (a -> ProduceMessage) -> Sink a IO ()
-- kafkaSink k t f = do
--     topic <- newKafkaTopic k t emptyTopicProps
--     CL.map f =$= CL.mapM_ (produceMessage topic UnassignedPartition)

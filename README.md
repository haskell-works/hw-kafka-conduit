# kafka-client-conduit

Conduit based API for `kafka-client`.

## Example

```haskell
import Control.Monad.IO.Class
import Conduit
import qualified Data.ByteString as BS
import Data.Conduit (Source, runConduitRes, (.|))
import qualified Data.Conduit.List as CL
import Kafka
import Kafka.Conduit.Consumer

main :: IO ()
main = do
  first5 <- runConduitRes $ creareKafkaStream .| L.take 5
  print first5

creareKafkaStream :: MonadResource m => Source m (Either KafkaError (ConsumerRecord BS.ByteString BS.ByteString))
creareKafkaStream = do
  kc  <- newConsumerConf (ConsumerGroupId "test_group") emptyKafkaProps
  tc  <- newConsumerTopicConf (TopicProps [("auto.offset.reset", "earliest")])
  kafkaSource kc tc (BrokersString "localhost:9092") (Timeout 30000) [TopicName "test_topic"]
```

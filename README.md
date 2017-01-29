# kafka-client-conduit
[![CircleCI](https://circleci.com/gh/haskell-works/kafka-client-conduit.svg?style=svg&circle-token=ff8f54bf644e2081b5683f3326559767b196814b)](https://circleci.com/gh/haskell-works/kafka-client-conduit)

Conduit based API for [kafka-client](https://github.com/haskell-works/kafka-client).

## Example

A working example can be found at [example/Main.hs](example/Main.hs)

#### Prerequisite
Running an example requires Kafka to be available at `localhost:9092`

`docker-compose` can be used for spinning up Kafka environment:

```
$ docker-compose up
```

### Running the example

```
stack build --exec kafka-client-conduit-example
```

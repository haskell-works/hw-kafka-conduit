# kafka-client-conduit
[![CircleCI](https://circleci.com/gh/haskell-works/hw-kafka-conduit.svg?style=svg&circle-token=ff8f54bf644e2081b5683f3326559767b196814b)](https://circleci.com/gh/haskell-works/hw-kafka-conduit)

Conduit based API for [kafka-client](https://github.com/haskell-works/hw-kafka-client).

## Ecosystem
HaskellWorks Kafka ecosystem is described here: https://github.com/haskell-works/hw-kafka

## Example
A working example can be found at [example/Main.hs](example/Main.hs)

### Prerequisites
Running an example requires Kafka to be available at `localhost:9092`

`docker-compose` can be used for spinning up Kafka environment:

```
$ docker-compose up
```

### Running the example

To build and run the example project:
```
$ stack build --flag hw-kafka-conduit:examples
```

or

```
$ stack build --exec kafka-conduit-example --flag hw-kafka-conduit:examples
```

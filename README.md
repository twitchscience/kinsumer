# Kinsumer

Native Go consumer for AWS Kinesis streams.

[![Build Status](https://travis-ci.org/twitchscience/kinsumer.svg?branch=master)](https://travis-ci.org/TwitchScience/kinsumer) [![Go Report Card](https://goreportcard.com/badge/github.com/twitchscience/kinsumer)](https://goreportcard.com/report/github.com/twitchscience/kinsumer)

## Rationale
There are several very good ways to consume Kinesis streams, primarily [The Amazon Kinesis Client Library](http://docs.aws.amazon.com/kinesis/latest/dev/developing-consumers-with-kcl.html), and it is recommended that be investigated as an option.

Kinsumer is designed for a cluster of Go clients that want each client to consume from multiple shards. Kinsumer is designed to be at-least-once with a strong effort to be exactly-once. Kinsumer by design does not attempt to keep shards on a specific client and will shuffle them around as needed.

## Behavior
Kinsumer is designed to suit a specific use case of kinesis consuming, specifically when you need to have multiple clients each handling multiple shards and you do not care which shard is being consumed by which client.

Kinsumer will rebalance shards to each client whenever it detects the list of shards or list of clients has changed, and does not attempt to keep shards on the same client.

If you are running multiple Kinsumer apps against a single stream, make sure to increase the throttleDelay to at least `50ms + (200ms * <the number of reader apps>)`. Note that Kinesis does not support more than two readers per writer on a fully utilized stream, so make sure you have enough stream capacity.

## Example
See `cmd/noopkinsumer` for a fully working example of a kinsumer client.

## Testing

### Testing with local test servers
By default the tests look for a dynamodb server at `localhost:4567` and kinesis server at `localhost:4568`


For example using [kinesalite](https://github.com/mhart/kinesalite) and [dynalite](https://github.com/mhart/dynalite)
```
kinesalite --port 4568 --createStreamMs 1 --deleteStreamMs 1 --updateStreamMs 1 --shardLimit 1000 &
dynalite --port 4567 --createTableMs 1 --deleteTableMs 1 --updateTableMs 1 &
```
Then `go test ./...`

### Testing with real aws resources
It's possible to run the test against real AWS resources, but the tests create and destroy resources, which can be finicky, and potentially expensive.

Make sure you have your credentials setup in a way that [aws-sdk-go](https://github.com/aws/aws-sdk-go) is happy with, or be running on an EC2 instance.

Then `go test . -dynamo_endpoint= -kinesis_endpoint=  -resource_change_timeout=30s`

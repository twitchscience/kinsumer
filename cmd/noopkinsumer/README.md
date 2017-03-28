# noopkinsumer

noopkinsumer is a bare-bones kinesis consumer using the kinsumer library. It consumes all the events on a
stream and does nothing with their data.

In addition to being a minimal example, noopkinsumer is useful for its side effect of writing stats to statsd.

## Resources

To use noopkinsumer you need a kinesis stream with data on it or being written to it, and three dynamo tables with
the following HASH keys

|TableName|DistKey|
|---------|-------|
|noopkinsumer_clients|ID (String)|
|noopkinsumer_checkpoints|Shard (String)|
|noopkinsumer_metadata|Key (String)|


// Copyright (c) 2016 Twitch Interactive

package kinsumer

import "errors"

var (
	// ErrRunTwice - Run() can only ever be run once
	ErrRunTwice = errors.New("run() can only ever be run once")
	// ErrNoKinesisInterface - Need a kinesis instance
	ErrNoKinesisInterface = errors.New("need a kinesis instance")
	// ErrNoDynamoInterface - Need a dynamodb instance
	ErrNoDynamoInterface = errors.New("need a dynamodb instance")
	// ErrNoStreamName - Need a kinesis stream name
	ErrNoStreamName = errors.New("need a kinesis stream name")
	// ErrNoApplicationName - Need an application name for the dynamo table names
	ErrNoApplicationName = errors.New("need an application name for the dynamo table names")
	// ErrThisClientNotInDynamo - Unable to find this client in the client list
	ErrThisClientNotInDynamo = errors.New("unable to find this client in the client list")
	// ErrNoShardsAssigned - We found shards, but got assigned none
	ErrNoShardsAssigned = errors.New("we found shards, but got assigned none")

	// ErrConfigInvalidThrottleDelay - ThrottleDelay config value must be at least 200ms
	ErrConfigInvalidThrottleDelay = errors.New("throttleDelay config value must be at least 200ms (preferably 250ms)")
	// ErrConfigInvalidCommitFrequency - CommitFrequency config value is mandatory
	ErrConfigInvalidCommitFrequency = errors.New("commitFrequency config value is mandatory")
	// ErrConfigInvalidShardCheckFrequency - ShardCheckFrequency config value is mandatory
	ErrConfigInvalidShardCheckFrequency = errors.New("shardCheckFrequency config value is mandatory")
	// ErrConfigInvalidLeaderActionFrequency - LeaderActionFrequency config value is mandatory
	ErrConfigInvalidLeaderActionFrequency = errors.New("leaderActionFrequency config value is mandatory and must be at least as long as ShardCheckFrequency")
	// ErrConfigInvalidBufferSize - BufferSize config value is mandatory
	ErrConfigInvalidBufferSize = errors.New("bufferSize config value is mandatory")
	// ErrConfigInvalidStats - Stats cannot be nil
	ErrConfigInvalidStats = errors.New("stats cannot be nil")
	// ErrConfigInvalidDynamoCapacity - Dynamo read/write capacity cannot be 0
	ErrConfigInvalidDynamoCapacity = errors.New("dynamo read/write capacity cannot be 0")

	// ErrStreamBusy - Stream is busy
	ErrStreamBusy = errors.New("stream is busy")
	// ErrNoSuchStream - No such stream
	ErrNoSuchStream = errors.New("no such stream")
)

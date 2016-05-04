// Copyright (c) 2016 Twitch Interactive

package kinsumer

import "time"

// A StatReceiver will have its methods called as operations
// happen inside a running kinsumer, and is useful for tracking
// the operation of the consumer.
//
// The methods will get called from multiple go routines and it is
// the implementors responsibility to handle thread synchronization
type StatReceiver interface {
	// Dynamo operations

	// Checkpoint is called every time a checkpoint is written to dynamodb
	Checkpoint()

	// EventToClient is called every time a record is returned to the client
	// `inserted` is the approximate time the record was inserted into kinesis
	// `retrieved` is the time when kinsumer retrieved the record from kinesis
	EventToClient(inserted, retrieved time.Time)

	// EventsFromKinesis is called every time a bunch of records is retrieved from
	// a kinesis shard.
	// `num` Number of records retrieved.
	// `shardID` ID of the shard that the records were retrieved from
	// `lag` How far the records are from the tip of the stream.
	EventsFromKinesis(num int, shardID string, lag time.Duration)
}

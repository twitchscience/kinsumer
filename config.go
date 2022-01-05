// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"time"
)

//TODO: Update documentation to include the defaults
//TODO: Update the 'with' methods' comments to be less ridiculous

// Config holds all configuration values for a single Kinsumer instance
type Config struct {
	stats  StatReceiver
	logger Logger

	// ---------- [ Per Shard Worker ] ----------
	// Time to sleep if no records are found
	throttleDelay time.Duration

	// Delay between commits to the checkpoint database
	commitFrequency time.Duration

	// Delay between tests for the client or shard numbers changing
	shardCheckFrequency time.Duration

	// Starting timestamp of the shard iterator, if "AT_TIMESTAMP" is the desired iterator type
	iteratorStartTimestamp *time.Time

	// ---------- [ For the leader (first client alphabetically) ] ----------
	// Time between leader actions
	leaderActionFrequency time.Duration

	// ---------- [ For the entire Kinsumer ] ----------
	// Size of the buffer for the combined records channel. When the channel fills up
	// the workers will stop adding new elements to the queue, so a slow client will
	// potentially fall behind the kinesis stream.
	bufferSize int

	// ---------- [ For the Dynamo DB tables ] ----------
	// Read and write capacity for the Dynamo DB tables when created
	// with CreateRequiredTables() call. If tables already exist because they were
	// created on a prevoius run or created manually, these parameters will not be used.
	dynamoReadCapacity  int64
	dynamoWriteCapacity int64
	// Time to wait between attempts to verify tables were created/deleted completely
	dynamoWaiterDelay time.Duration
}

// NewConfig returns a default Config struct
func NewConfig() Config {
	return Config{
		throttleDelay:         250 * time.Millisecond,
		commitFrequency:       1000 * time.Millisecond,
		shardCheckFrequency:   1 * time.Minute,
		leaderActionFrequency: 1 * time.Minute,
		bufferSize:            100,
		stats:                 &NoopStatReceiver{},
		dynamoReadCapacity:    10,
		dynamoWriteCapacity:   10,
		dynamoWaiterDelay:     3 * time.Second,
		logger:                &DefaultLogger{},
	}
}

// WithThrottleDelay returns a Config with a modified throttle delay
func (c Config) WithThrottleDelay(delay time.Duration) Config {
	c.throttleDelay = delay
	return c
}

// WithCommitFrequency returns a Config with a modified commit frequency
func (c Config) WithCommitFrequency(commitFrequency time.Duration) Config {
	c.commitFrequency = commitFrequency
	return c
}

// WithShardCheckFrequency returns a Config with a modified shard check frequency
func (c Config) WithShardCheckFrequency(shardCheckFrequency time.Duration) Config {
	c.shardCheckFrequency = shardCheckFrequency
	return c
}

// WithLeaderActionFrequency returns a Config with a modified leader action frequency
func (c Config) WithLeaderActionFrequency(leaderActionFrequency time.Duration) Config {
	c.leaderActionFrequency = leaderActionFrequency
	return c
}

// WithBufferSize returns a Config with a modified buffer size
func (c Config) WithBufferSize(bufferSize int) Config {
	c.bufferSize = bufferSize
	return c
}

// WithStats returns a Config with a modified stats
func (c Config) WithStats(stats StatReceiver) Config {
	c.stats = stats
	return c
}

// WithIteratorStartTimestamp returns a Config with a modified iteratorStartTimestamp
func (c Config) WithIteratorStartTimestamp(timestamp *time.Time) Config {
	c.iteratorStartTimestamp = timestamp
	return c
}

// WithDynamoReadCapacity returns a Config with a modified dynamo read capacity
func (c Config) WithDynamoReadCapacity(readCapacity int64) Config {
	c.dynamoReadCapacity = readCapacity
	return c
}

// WithDynamoWriteCapacity returns a Config with a modified dynamo write capacity
func (c Config) WithDynamoWriteCapacity(writeCapacity int64) Config {
	c.dynamoWriteCapacity = writeCapacity
	return c
}

// WithDynamoWaiterDelay returns a Config with a modified dynamo waiter delay
func (c Config) WithDynamoWaiterDelay(delay time.Duration) Config {
	c.dynamoWaiterDelay = delay
	return c
}

// WithLogger returns a Config with a modified logger
func (c Config) WithLogger(logger Logger) Config {
	c.logger = logger
	return c
}

// Verify that a config struct has sane and valid values
func validateConfig(c *Config) error {
	if c.throttleDelay < 200*time.Millisecond {
		return ErrConfigInvalidThrottleDelay
	}

	if c.commitFrequency == 0 {
		return ErrConfigInvalidCommitFrequency
	}

	if c.shardCheckFrequency == 0 {
		return ErrConfigInvalidShardCheckFrequency
	}

	if c.leaderActionFrequency == 0 {
		return ErrConfigInvalidLeaderActionFrequency
	}

	if c.shardCheckFrequency > c.leaderActionFrequency {
		return ErrConfigInvalidLeaderActionFrequency
	}

	if c.bufferSize == 0 {
		return ErrConfigInvalidBufferSize
	}

	if c.stats == nil {
		return ErrConfigInvalidStats
	}

	if c.dynamoReadCapacity == 0 || c.dynamoWriteCapacity == 0 {
		return ErrConfigInvalidDynamoCapacity
	}

	if c.logger == nil {
		return ErrConfigInvalidLogger
	}

	return nil
}

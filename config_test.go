// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigDefault(t *testing.T) {
	config := NewConfig()
	err := validateConfig(&config)
	require.NoError(t, err)
	require.Nil(t, config.iteratorStartTimestamp)
}

func TestConfigErrors(t *testing.T) {
	var (
		config Config
		err    error
	)
	config = NewConfig().WithBufferSize(0)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidBufferSize.Error())

	config = NewConfig().WithThrottleDelay(0)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidThrottleDelay.Error())

	config = NewConfig().WithCommitFrequency(0)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidCommitFrequency.Error())

	config = NewConfig().WithShardCheckFrequency(0)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidShardCheckFrequency.Error())

	config = NewConfig().WithLeaderActionFrequency(0)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidLeaderActionFrequency.Error())

	config = NewConfig().WithLeaderActionFrequency(time.Second).WithShardCheckFrequency(time.Minute)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidLeaderActionFrequency.Error())

	config = NewConfig().WithBufferSize(0)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidBufferSize.Error())

	config = NewConfig().WithStats(nil)
	err = validateConfig(&config)
	require.EqualError(t, err, ErrConfigInvalidStats.Error())
}

func TestConfigWithMethods(t *testing.T) {
	stats := &NoopStatReceiver{}
	tstamp := time.Now()
	config := NewConfig().
		WithBufferSize(1).
		WithCommitFrequency(1 * time.Second).
		WithShardCheckFrequency(1 * time.Second).
		WithLeaderActionFrequency(1 * time.Second).
		WithThrottleDelay(1 * time.Second).
		WithStats(stats).
		WithIteratorStartTimestamp(&tstamp)

	err := validateConfig(&config)
	require.NoError(t, err)

	require.Equal(t, 1, config.bufferSize)
	require.Equal(t, 1*time.Second, config.throttleDelay)
	require.Equal(t, 1*time.Second, config.commitFrequency)
	require.Equal(t, 1*time.Second, config.shardCheckFrequency)
	require.Equal(t, 1*time.Second, config.leaderActionFrequency)
	require.Equal(t, stats, config.stats)
	require.Equal(t, &tstamp, config.iteratorStartTimestamp)
}

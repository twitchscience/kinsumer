// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	// getRecordsLimit is the max number of records in a single request. This effectively limits the
	// total processing speed to getRecordsLimit*5/n where n is the number of parallel clients trying
	// to consume from the same kinesis stream
	getRecordsLimit = 10000 // 10,000 is the max according to the docs

	// maxErrorRetries is how many times we will retry on a shard error
	maxErrorRetries = 3

	// errorSleepDuration is how long we sleep when an error happens, this is multiplied by the number
	// of retries to give a minor backoff behavior
	errorSleepDuration = 1 * time.Second
)

type consumeEvent struct {
	Records        []*kinesis.Record
	Lag            time.Duration
	SequenceNumber string
	Finished       bool
}

// getShardIterator gets a shard iterator after the last sequence number we read or at the start of the stream
func getShardIterator(k kinesisiface.KinesisAPI, streamName string, shardID string, sequenceNumber string) (string, error) {
	shardIteratorType := kinesis.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" {
		shardIteratorType = kinesis.ShardIteratorTypeTrimHorizon
		ps = nil
	} else if sequenceNumber == "LATEST" {
		shardIteratorType = kinesis.ShardIteratorTypeLatest
		ps = nil
	}

	resp, err := k.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shardID),
		ShardIteratorType:      &shardIteratorType,
		StartingSequenceNumber: ps,
		StreamName:             aws.String(streamName),
	})
	return aws.StringValue(resp.ShardIterator), err
}

// getRecords returns the next records and shard iterator from the given shard iterator
func getRecords(k kinesisiface.KinesisAPI, iterator string) (records []*kinesis.Record, nextIterator string, lag time.Duration, err error) {
	params := &kinesis.GetRecordsInput{
		Limit:         aws.Int64(getRecordsLimit),
		ShardIterator: aws.String(iterator),
	}

	output, err := k.GetRecords(params)

	if err != nil {
		return nil, "", 0, err
	}

	records = output.Records
	nextIterator = aws.StringValue(output.NextShardIterator)
	lag = time.Duration(aws.Int64Value(output.MillisBehindLatest)) * time.Millisecond

	return records, nextIterator, lag, nil
}

// captureShard blocks until we capture the given shardID
func (k *Kinsumer) captureShard(shardID string) (*checkpointer, error) {
	// Attempt to capture the shard in dynamo
	for {
		// Ask the checkpointer to capture the shard
		checkpointer, err := capture(
			shardID,
			k.checkpointTableName,
			k.dynamodb,
			k.clientName,
			k.clientID,
			k.maxAgeForClientRecord,
			k.config.stats)
		if err != nil {
			return nil, err
		}

		if checkpointer != nil {
			return checkpointer, nil
		}

		// Throttle requests so that we don't hammer dynamo
		select {
		case <-k.stop:
			// If we are told to stop consuming we should stop attempting to capture
			return nil, nil
		case <-time.After(k.config.throttleDelay):
		}
	}
}

// consume is a blocking call that captures then consumes the given shard in a loop.
// It is also responsible for writing out the checkpoint updates to dynamo.
// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consume(shardID string) {
	defer k.waitGroup.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := k.stop

	// commitTicker is used to periodically commit, so that we don't hammer dynamo every time
	// a shard wants to be check pointed
	commitTicker := time.NewTicker(k.config.commitFrequency)
	defer commitTicker.Stop()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	if err != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "captureShard", err: err}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	// Make sure we release the shard when we are done.
	defer func() {
		innerErr := checkpointer.release()
		if innerErr != nil {
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.release", err: innerErr}
			return
		}
	}()

	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-commitTicker.C:
			}

			finishCommitted, err := checkpointer.commit()
			if err != nil {
				k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.commit", err: err}
				return
			}
			if finishCommitted {
				return
			}
		}
	}()

	sequenceNumber := checkpointer.sequenceNumber

	evtCh := k.consumePolling(ctx, shardID, sequenceNumber)

mainloop:
	// Continue processing until both the checkpointer and event goroutines are done
	// Signal to those goroutines to stop when either of them stop, or when a stop request comes in
	for commitDone != nil || evtCh != nil {
		select {
		case <-stop:
			cancel()
			stop = nil
		case <-commitDone:
			cancel()
			commitDone = nil
		case e, ok := <-evtCh:
			if !ok {
				cancel()
				evtCh = nil
				continue mainloop
			}
			if e.Finished {
				checkpointer.finish(e.SequenceNumber)
			}

			// Put all the records we got onto the channel
			k.config.stats.EventsFromKinesis(len(e.Records), shardID, e.Lag)
			retrievedAt := time.Now()
			for _, record := range e.Records {
				r := &consumedRecord{
					record:       record,
					checkpointer: checkpointer,
					retrievedAt:  retrievedAt,
				}
				// Do a blocking send, unless we're stopping in which case do a non-blocking send
				// We have to check both ctx.Done and stop since we don't know if stop has been processed yet
				select {
				case <-ctx.Done():
				case <-stop:
				case k.records <- r:
				}
			}
		}
	}
}

func (k *Kinsumer) consumePolling(ctx context.Context, shardID string, sequenceNumber string) <-chan consumeEvent {
	ch := make(chan consumeEvent)

	go func() {
		defer close(ch)

		// no throttle on the first request.
		nextThrottle := time.After(0)
		retryCount := 0

		for {
			// Get the starting shard iterator
			iterator, err := getShardIterator(k.kinesis, k.streamName, shardID, sequenceNumber)
			if err != nil {
				k.shardErrors <- shardConsumerError{shardID: shardID, action: "getShardIterator", err: err}
				return
			}

		getrecordloop:
			for {
				select {
				case <-ctx.Done():
					return
				case <-nextThrottle:
				}

				// Reset the nextThrottle
				nextThrottle = time.After(k.config.throttleDelay)

				// Get records from kinesis
				records, next, lag, err := getRecords(k.kinesis, iterator)
				if err != nil {
					if awsErr, ok := err.(awserr.Error); ok {
						// Iterators expire after 5 minutes, which can happen if Next() is called too slowly
						// Rather than error we can generate a new iterator and recover
						if awsErr.Code() == "ExpiredIteratorException" {
							break getrecordloop
						}

						k.config.logger.Log("Got error: %s (%s) retry count is %d / %d", awsErr.Message(), awsErr.OrigErr(), retryCount, maxErrorRetries)
						if retryCount < maxErrorRetries {
							retryCount++

							// casting retryCount here to time.Duration purely for the multiplication, there is
							// no meaning to retryCount nanoseconds
							time.Sleep(errorSleepDuration * time.Duration(retryCount))
							continue getrecordloop
						}
					}
					k.shardErrors <- shardConsumerError{shardID: shardID, action: "getRecords", err: err}
					return
				}

				retryCount = 0
				iterator = next
				if len(records) > 0 {
					sequenceNumber = aws.StringValue(records[len(records)-1].SequenceNumber)
				}
				finished := iterator == ""

				ev := consumeEvent{
					Records:        records,
					Lag:            lag,
					SequenceNumber: sequenceNumber,
					Finished:       finished,
				}

				select {
				case <-ctx.Done():
					return
				case ch <- ev:
				}

				if finished {
					return
				}
			}
		}
	}()

	return ch
}

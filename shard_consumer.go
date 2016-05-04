// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"log"
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
	maxErrorRetries = 5

	// errorSleepDuration is how long we sleep when an error happens, this is multiplied by the number
	// of retries to give a minor backoff behavior
	errorSleepDuration = 1 * time.Second
)

func getShardIterator(k kinesisiface.KinesisAPI, streamName *string, shardID *string, sequenceNumber string) (*string, error) {
	shardIteratorType := kinesis.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" {
		shardIteratorType = kinesis.ShardIteratorTypeTrimHorizon
		ps = nil
	}

	resp, err := k.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                shardID,
		ShardIteratorType:      &shardIteratorType,
		StartingSequenceNumber: ps,
		StreamName:             streamName,
	})
	return resp.ShardIterator, err
}

func getRecords(k kinesisiface.KinesisAPI, iterator *string) (records []*kinesis.Record, nextIterator *string, lag time.Duration, err error) {
	params := &kinesis.GetRecordsInput{
		Limit:         aws.Int64(getRecordsLimit),
		ShardIterator: iterator,
	}

	output, err := k.GetRecords(params)

	if err != nil {
		return nil, nil, 0, err
	}

	records = output.Records
	nextIterator = output.NextShardIterator
	lag = time.Duration(aws.Int64Value(output.MillisBehindLatest)) * time.Millisecond

	return records, nextIterator, lag, nil
}

func (k *Kinsumer) captureShard(shardID *string) (*checkpointer, error) {
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

// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consume(shardID *string) {
	defer k.waitGroup.Done()

	// commitTicker is used to periodically commit, so that we don't hammer dynamo every time
	// a shard wants to be check pointed
	commitTicker := time.NewTicker(k.config.commitFrequency)
	defer commitTicker.Stop()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	if err != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, err: err}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.sequenceNumber

	// Make sure we release the shard when we are done.
	defer func() {
		innerErr := checkpointer.release()
		if innerErr != nil {
			k.shardErrors <- shardConsumerError{shardID: shardID, err: innerErr}
			return
		}
	}()

	// Get the starting shard iterator
	iterator, err := getShardIterator(k.kinesis, &k.streamName, shardID, sequenceNumber)
	if err != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, err: err}
		return
	}

	// nextThrottleDelay is how long we delay requests to kinesis.
	nextThrottleDelay := k.config.throttleDelay

	retryCount := 0

mainloop:
	for {
		// We have reached the end of the shard's data. Stop processing.
		if iterator == nil || *iterator == "" {
			//TODO: Note that right now this will cause uneven work on clients as the balancing algorithm won't
			//      take fully consumed (split/merged) shards into account.
			break
		}

		// Handle async actions, and throttle requests to keep kinesis happy
		select {
		case <-k.stop:
			return
		case <-commitTicker.C:
			err := checkpointer.commit()
			if err != nil {
				k.shardErrors <- shardConsumerError{shardID: shardID, err: err}
				return
			}
		case <-time.After(nextThrottleDelay):
		}

		// Reset the throttleDelay
		nextThrottleDelay = k.config.throttleDelay

		// Get records from kinesis
		records, next, lag, err := getRecords(k.kinesis, iterator)

		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				switch awsErr.Code() {
				case "ProvisionedThroughputExceededException", "LimitExceededException":
					// No need to do anything in this situation, just recycle and wait for the throttleDelay
					continue mainloop
				default:
					if retryCount > 0 {
						log.Println("Got error", awsErr.Message(), "retry count is", retryCount, "/", maxErrorRetries)
					}
					if retryCount < maxErrorRetries {
						retryCount++

						// casting retryCount here to time.Duration purely for the multiplication, there is
						// no meaning to retryCount nanoseconds
						time.Sleep(errorSleepDuration * time.Duration(retryCount))
						continue mainloop
					}
				}
			}
			k.shardErrors <- shardConsumerError{shardID: shardID, err: err}
			return
		}
		retryCount = 0

		// Put all the records we got onto the channel
		k.config.stats.EventsFromKinesis(len(records), aws.StringValue(shardID), lag)
		if len(records) > 0 {
			retrievedAt := time.Now()
			for _, record := range records {
				select {
				case <-k.stop:
					return
				case k.records <- &consumedRecord{
					record:       record,
					checkpointer: checkpointer,
					retrievedAt:  retrievedAt,
				}:
				}
			}

			// Since we got records, lets hit kinesis again.
			nextThrottleDelay = 0
		}
		iterator = next
	}
}

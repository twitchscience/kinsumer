// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	leaderKey       = "Leader"
	shardCacheKey   = "ShardCache"
	conditionalFail = "ConditionalCheckFailedException"
)

type shardCacheRecord struct {
	Key        string   // must be "ShardCache"
	ShardIDs   []string // Slice of unfinished shard IDs
	LastUpdate int64    // timestamp of last update

	// Debug versions of LastUpdate
	LastUpdateRFC string
}

// becomeLeader starts the leadership goroutine with a channel to stop it.
// TODO(dwe): Factor out dependencies and unit test
func (k *Kinsumer) becomeLeader() {
	if k.isLeader {
		return
	}
	k.leaderLost = make(chan bool)
	k.leaderWG.Add(1)
	go func() {
		defer k.leaderWG.Done()
		leaderActions := time.NewTicker(k.config.leaderActionFrequency)
		defer func() {
			leaderActions.Stop()
			err := k.deregisterLeadership()
			if err != nil {
				k.errors <- fmt.Errorf("error deregistering leadership: %v", err)
			}
		}()
		ok, err := k.registerLeadership()
		if err != nil {
			k.errors <- fmt.Errorf("error registering initial leadership: %v", err)
		}
		// Perform leadership actions immediately if we became leader. If we didn't
		// become leader yet, wait until the first tick to try again.
		if ok {
			err = k.performLeaderActions()
			if err != nil {
				k.errors <- fmt.Errorf("error performing initial leader actions: %v", err)
			}
		}
		for {
			select {
			case <-leaderActions.C:
				ok, err := k.registerLeadership()
				if err != nil {
					k.errors <- fmt.Errorf("Error registering leadership: %v", err)
				}
				if !ok {
					continue
				}
				err = k.performLeaderActions()
				if err != nil {
					k.errors <- fmt.Errorf("Error performing repeated leader actions: %v", err)
				}
			case <-k.leaderLost:
				return
			}
		}
	}()
	k.isLeader = true
}

// unbecomeLeader stops the leadership goroutine.
func (k *Kinsumer) unbecomeLeader() {
	if !k.isLeader {
		return
	}
	if k.leaderLost == nil {
		log.Printf("Lost leadership but k.leaderLost was nil")
	} else {
		close(k.leaderLost)
		k.leaderWG.Wait()
		k.leaderLost = nil
	}
	k.isLeader = false
}

// performLeaderActions updates the shard ID cache and reaps old clients
// TODO(dwe): Factor out dependencies and unit test
func (k *Kinsumer) performLeaderActions() error {
	shardCache, err := loadShardCacheFromDynamo(k.dynamodb, k.metadataTableName)
	if err != nil {
		return fmt.Errorf("error loading shard cache from dynamo: %v", err)
	}
	cachedShardIDs := shardCache.ShardIDs
	now := time.Now().UnixNano()
	if now-shardCache.LastUpdate < k.config.leaderActionFrequency.Nanoseconds() {
		return nil
	}
	curShardIDs, err := loadShardIDsFromKinesis(k.kinesis, k.streamName)
	if err != nil {
		return fmt.Errorf("error loading shard IDs from kinesis: %v", err)
	}

	checkpoints, err := loadCheckpoints(k.dynamodb, k.checkpointTableName)
	if err != nil {
		return fmt.Errorf("error loading shard IDs from dynamo: %v", err)
	}

	updatedShardIDs, changed := diffShardIDs(curShardIDs, cachedShardIDs, checkpoints)
	if changed {
		err = k.setCachedShardIDs(updatedShardIDs)
		if err != nil {
			return fmt.Errorf("error caching shard IDs to dynamo: %v", err)
		}
	}

	err = reapClients(k.dynamodb, k.clientsTableName)
	if err != nil {
		return fmt.Errorf("error reaping old clients: %v", err)
	}

	return nil
}

// setCachedShardIDs updates the shard ID cache in dynamo.
func (k *Kinsumer) setCachedShardIDs(shardIDs []string) error {
	if len(shardIDs) == 0 {
		return nil
	}
	now := time.Now()
	item, err := dynamodbattribute.MarshalMap(&shardCacheRecord{
		Key:           shardCacheKey,
		ShardIDs:      shardIDs,
		LastUpdate:    now.UnixNano(),
		LastUpdateRFC: now.UTC().Format(time.RFC1123Z),
	})
	if err != nil {
		return fmt.Errorf("error marshalling map: %v", err)
	}

	_, err = k.dynamodb.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(k.metadataTableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("error updating shard cache: %v", err)
	}
	return nil
}

// diffShardIDs takes the current shard IDs and cached shards and returns the new sorted cache, ignoring
// finished shards correctly.
func diffShardIDs(curShardIDs, cachedShardIDs []string, checkpoints map[string]*checkpointRecord) (updatedShardIDs []string, changed bool) {
	// Look for differences, ignoring Finished shards.
	cur := make(map[string]bool)
	for _, s := range curShardIDs {
		cur[s] = true
	}
	for _, s := range cachedShardIDs {
		if cur[s] {
			delete(cur, s)
			// Drop the shard if it's been finished.
			if c, ok := checkpoints[s]; ok && c.Finished != nil {
				changed = true
			} else {
				updatedShardIDs = append(updatedShardIDs, s)
			}
		} else {
			// If a shard is no longer returned by DescribeStream, drop it.
			changed = true
		}
	}
	for s := range cur {
		// If the shard is returned by DescribeStream and not already Finished, add it.
		if c, ok := checkpoints[s]; !ok || c.Finished == nil {
			updatedShardIDs = append(updatedShardIDs, s)
			changed = true
		}
	}
	sort.Strings(updatedShardIDs)
	return
}

// deregisterLeadership marks us as no longer the leader in dynamo.
func (k *Kinsumer) deregisterLeadership() error {
	now := time.Now()
	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":ID":            aws.String(k.clientID),
		":lastUpdate":    aws.Int64(now.UnixNano()),
		":lastUpdateRFC": aws.String(now.UTC().Format(time.RFC1123Z)),
	})
	if err != nil {
		return fmt.Errorf("error marshaling deregisterLeadership ExpressionAttributeValues: %v", err)
	}
	_, err = k.dynamodb.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(k.metadataTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {S: aws.String(leaderKey)},
		},
		ConditionExpression:       aws.String("ID = :ID"),
		UpdateExpression:          aws.String("REMOVE ID SET LastUpdate = :lastUpdate, LastUpdateRFC = :lastUpdateRFC"),
		ExpressionAttributeValues: attrVals,
	})
	if err != nil {
		// It's ok if we never actually became leader.
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == conditionalFail {
			return nil
		}
	}
	return err
}

// registerLeadership marks us as the leader or just refreshes LastUpdate in dynamo, returning false if
// another node is the leader.
func (k *Kinsumer) registerLeadership() (bool, error) {
	now := time.Now()
	cutoff := now.Add(-k.maxAgeForLeaderRecord).UnixNano()
	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":ID":     aws.String(k.clientID),
		":cutoff": aws.Int64(cutoff),
	})
	if err != nil {
		return false, fmt.Errorf("error marshaling registerLeadership ExpressionAttributeValues: %v", err)
	}
	item, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		"Key":           aws.String(leaderKey),
		"ID":            aws.String(k.clientID),
		"Name":          aws.String(k.clientName),
		"LastUpdate":    aws.Int64(now.UnixNano()),
		"LastUpdateRFC": aws.String(now.UTC().Format(time.RFC1123Z)),
	})
	if err != nil {
		return false, fmt.Errorf("error marshaling registerLeadership Item: %v", err)
	}
	_, err = k.dynamodb.PutItem(&dynamodb.PutItemInput{
		TableName:                 aws.String(k.metadataTableName),
		Item:                      item,
		ConditionExpression:       aws.String("ID = :ID OR attribute_not_exists(ID) OR LastUpdate <= :cutoff"),
		ExpressionAttributeValues: attrVals,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == conditionalFail {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// loadShardIDsFromKinesis returns a sorted slice of shardIDs from kinesis.
// This function uses kinesis.DescribeStream, which has a very low throttling limit of 10/s per account.
// To avoid hitting that limit, unless you need an as-recent-as-possible list,
// you should use the cache, returned by loadShardIDsFromDynamo below.
//TODO: Write unit test - needs kinesis mocking
func loadShardIDsFromKinesis(kin kinesisiface.KinesisAPI, streamName string) ([]string, error) {
	var (
		innerError error
		shards     []*kinesis.Shard
	)

	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
		Limit:      aws.Int64(10000),
	}

	err := kin.DescribeStreamPages(params, func(page *kinesis.DescribeStreamOutput, _ bool) bool {
		if page == nil || page.StreamDescription == nil {
			innerError = ErrKinesisCantDescribeStream
			return false
		}

		switch aws.StringValue(page.StreamDescription.StreamStatus) {
		case "CREATING":
			innerError = ErrKinesisBeingCreated
			return false
		case "DELETING":
			innerError = ErrKinesisBeingDeleted
			return false
		}
		shards = append(shards, page.StreamDescription.Shards...)
		return aws.BoolValue(page.StreamDescription.HasMoreShards)
	})

	if innerError != nil {
		return nil, innerError
	}

	if err != nil {
		return nil, err
	}

	shardIDs := make([]string, len(shards))
	for i, s := range shards {
		shardIDs[i] = aws.StringValue(s.ShardId)
	}
	sort.Strings(shardIDs)

	return shardIDs, nil
}

// loadShardIDsFromDynamo returns the sorted slice of shardIDs from the metadata table in dynamo.
func loadShardIDsFromDynamo(db dynamodbiface.DynamoDBAPI, tableName string) ([]string, error) {
	record, err := loadShardCacheFromDynamo(db, tableName)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	return record.ShardIDs, nil
}

// loadShardCacheFromDynamo returns the ShardCache record from the metadata table in dynamo.
func loadShardCacheFromDynamo(db dynamodbiface.DynamoDBAPI, tableName string) (*shardCacheRecord, error) {
	resp, err := db.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {S: aws.String(shardCacheKey)},
		},
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ResourceNotFoundException" {
			return nil, nil
		}
		return nil, err
	}
	var record shardCacheRecord
	if err = dynamodbattribute.UnmarshalMap(resp.Item, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

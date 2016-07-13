// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Note: Not thread safe!

type checkpointer struct {
	shardID               *string
	tableName             string
	dynamodb              dynamodbiface.DynamoDBAPI
	sequenceNumber        string
	ownerName             string
	ownerID               string
	maxAgeForClientRecord time.Duration
	stats                 StatReceiver
	captured              bool
	dirty                 bool
	mutex                 sync.Mutex
}

// Base values for the dynamo table, every record has to have these values.
type checkpointRecord struct {
	Shard          string
	SequenceNumber *string
	LastUpdate     int64
	OwnerName      *string

	// Columns added to the table that are never used for decision making in the
	// library, rather they are useful for manual troubleshooting
	OwnerID       *string
	LastUpdateRFC string
}

func capture(
	shardID *string,
	tableName string,
	dynamodbiface dynamodbiface.DynamoDBAPI,
	ownerName string,
	ownerID string,
	maxAgeForClientRecord time.Duration,
	stats StatReceiver) (*checkpointer, error) {

	cutoff := time.Now().Add(-maxAgeForClientRecord).UnixNano()

	// Grab the entry from dynamo assuming there is one
	resp, err := dynamodbiface.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"Shard": {S: shardID},
		},
	})

	if err != nil {
		return nil, err
	}

	// Convert to struct so we can work with the values
	var record checkpointRecord
	if err = dynamodbattribute.ConvertFromMap(resp.Item, &record); err != nil {
		return nil, err
	}

	// If the record is marked as owned by someone else, and has not expired
	if record.OwnerID != nil && record.LastUpdate > cutoff {
		// We fail to capture it
		return nil, nil
	}

	// Make sure the Shard is set in case there was no record
	record.Shard = aws.StringValue(shardID)

	// Mark us as the owners
	record.OwnerID = &ownerID
	record.OwnerName = &ownerName

	// Update timestamp
	now := time.Now()
	record.LastUpdate = now.UnixNano()
	record.LastUpdateRFC = now.UTC().Format(time.RFC1123Z)

	item, err := dynamodbattribute.ConvertToMap(record)
	if err != nil {
		return nil, err
	}

	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":cutoff":   aws.Int64(cutoff),
		":nullType": aws.String("NULL"),
	})
	if err != nil {
		return nil, err
	}
	if _, err = dynamodbiface.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
		// The OwnerID doesn't exist if the entry doesn't exist, but PutItem with a marshaled
		// checkpointRecord sets a nil OwnerID to the NULL type.
		ConditionExpression: aws.String(
			"attribute_not_exists(OwnerID) OR attribute_type(OwnerID, :nullType) OR LastUpdate <= :cutoff"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ConditionalCheckFailedException" {
			// We failed to capture it
			return nil, nil
		}
		return nil, err
	}

	checkpointer := &checkpointer{
		shardID:               shardID,
		tableName:             tableName,
		dynamodb:              dynamodbiface,
		ownerName:             ownerName,
		ownerID:               ownerID,
		stats:                 stats,
		sequenceNumber:        aws.StringValue(record.SequenceNumber),
		maxAgeForClientRecord: maxAgeForClientRecord,
		captured:              true,
	}

	return checkpointer, nil
}

func (cp *checkpointer) commit() error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	if !cp.dirty {
		return nil
	}
	now := time.Now()

	sn := &cp.sequenceNumber
	if cp.sequenceNumber == "" {
		// We are not allowed to pass empty strings to dynamo, so instead pass a nil *string
		// to 'unset' it
		sn = nil
	}

	item, err := dynamodbattribute.ConvertToMap(checkpointRecord{
		Shard:          aws.StringValue(cp.shardID),
		SequenceNumber: sn,
		OwnerID:        &cp.ownerID,
		OwnerName:      &cp.ownerName,
		LastUpdate:     now.UnixNano(),
		LastUpdateRFC:  now.UTC().Format(time.RFC1123Z),
	})
	if err != nil {
		return err
	}

	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":ownerID": aws.String(cp.ownerID),
	})
	if err != nil {
		return err
	}
	if _, err = cp.dynamodb.PutItem(&dynamodb.PutItemInput{
		TableName:                 aws.String(cp.tableName),
		Item:                      item,
		ConditionExpression:       aws.String("OwnerID = :ownerID"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		return fmt.Errorf("error committing checkpoint: %s", err)
	}

	if sn != nil {
		cp.stats.Checkpoint()
	}
	cp.dirty = false

	return nil
}

func (cp *checkpointer) release() error {
	now := time.Now()

	sn := &cp.sequenceNumber
	if *sn == "" {
		sn = nil
	}
	item, err := dynamodbattribute.ConvertToMap(checkpointRecord{
		Shard:          aws.StringValue(cp.shardID),
		SequenceNumber: sn,
		LastUpdate:     now.UnixNano(),
		LastUpdateRFC:  now.UTC().Format(time.RFC1123Z),
	})
	if err != nil {
		return err
	}

	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":ownerID": aws.String(cp.ownerID),
	})
	if err != nil {
		return err
	}
	if _, err = cp.dynamodb.PutItem(&dynamodb.PutItemInput{
		TableName:                 aws.String(cp.tableName),
		Item:                      item,
		ConditionExpression:       aws.String("OwnerID = :ownerID"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		return fmt.Errorf("error releasing checkpoint: %s", err)
	}

	if sn != nil {
		cp.stats.Checkpoint()
	}

	cp.captured = false

	return nil
}

func (cp *checkpointer) update(sequenceNumber string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.dirty = cp.dirty || cp.sequenceNumber != sequenceNumber
	cp.sequenceNumber = sequenceNumber
}

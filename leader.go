// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	leaderKey       = "Leader"
	conditionalFail = "ConditionalCheckFailedException"
)

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
			_, err = k.performLeaderActions()
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
				_, err = k.performLeaderActions()
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

// performLeaderActions is a temporary no-op.
// TODO(dwe): Factor out dependencies and unit test
func (k *Kinsumer) performLeaderActions() ([]string, error) {
	return nil, nil
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

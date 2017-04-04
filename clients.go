// Copyright (c) 2016 Twitch Interactive

package kinsumer

//TODO: The filename is bad

import (
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const clientReapAge = 48 * time.Hour

type clientRecord struct {
	ID         string
	LastUpdate int64

	// Columns added to the table that are never used for decision making in the
	// library, rather they are useful for manual troubleshooting
	Name          string
	LastUpdateRFC string
}

type sortableClients []clientRecord

func (sc sortableClients) Len() int {
	return len(sc)
}

func (sc sortableClients) Less(left, right int) bool {
	return sc[left].ID < sc[right].ID
}

func (sc sortableClients) Swap(left, right int) {
	sc[left], sc[right] = sc[right], sc[left]
}

// registerWithClientsTable adds or updates our client with a current LastUpdate in dynamo
func registerWithClientsTable(db dynamodbiface.DynamoDBAPI, id, name, tableName string) error {
	now := time.Now()
	item, err := dynamodbattribute.MarshalMap(clientRecord{
		ID:            id,
		Name:          name,
		LastUpdate:    now.UnixNano(),
		LastUpdateRFC: now.UTC().Format(time.RFC1123Z),
	})

	if err != nil {
		return err
	}

	if _, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	}); err != nil {
		return err
	}

	return nil
}

// deregisterWithClientsTable deletes our client from dynamo
func deregisterFromClientsTable(db dynamodbiface.DynamoDBAPI, id, tableName string) error {
	idStruct := struct{ ID string }{ID: id}
	item, err := dynamodbattribute.MarshalMap(idStruct)

	if err != nil {
		return err
	}

	if _, err = db.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key:       item,
	}); err != nil {
		return err
	}

	return nil
}

// getClients returns a sorted list of all recently-updated clients in dynamo
func getClients(db dynamodbiface.DynamoDBAPI, name string, tableName string, maxAgeForClientRecord time.Duration) (clients []clientRecord, err error) {
	filterExpression := "LastUpdate > :cutoff"
	cutoff := strconv.FormatInt(time.Now().Add(-maxAgeForClientRecord).UnixNano(), 10)

	params := &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		ConsistentRead:   aws.Bool(true),
		FilterExpression: aws.String(filterExpression),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":cutoff": {N: &cutoff},
		},
	}

	var innerError error
	err = db.ScanPages(params, func(p *dynamodb.ScanOutput, lastPage bool) (shouldContinue bool) {
		for _, item := range p.Items {
			var record clientRecord
			innerError = dynamodbattribute.UnmarshalMap(item, &record)
			if innerError != nil {
				return false
			}
			clients = append(clients, record)
		}

		return !lastPage
	})

	if innerError != nil {
		return nil, innerError
	}

	if err != nil {
		return nil, err
	}

	sort.Sort(sortableClients(clients))
	return clients, nil
}

// reapClients deletes any sufficiently old clients from dynamo
func reapClients(db dynamodbiface.DynamoDBAPI, tableName string) error {
	filterExpression := "LastUpdate < :cutoff"
	cutoff := strconv.FormatInt(time.Now().Add(-clientReapAge).UnixNano(), 10)

	params := &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		ConsistentRead:   aws.Bool(true),
		FilterExpression: aws.String(filterExpression),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":cutoff": {N: &cutoff},
		},
	}

	var clients []clientRecord
	var innerError error
	err := db.ScanPages(params, func(p *dynamodb.ScanOutput, lastPage bool) (shouldContinue bool) {
		for _, item := range p.Items {
			var record clientRecord
			innerError = dynamodbattribute.UnmarshalMap(item, &record)
			if innerError != nil {
				return false
			}
			clients = append(clients, record)
		}

		return !lastPage
	})

	if innerError != nil {
		return innerError
	}

	if err != nil {
		return err
	}

	for _, client := range clients {
		idStruct := struct{ ID string }{ID: client.ID}
		item, err := dynamodbattribute.MarshalMap(idStruct)
		if err != nil {
			return err
		}
		if _, err = db.DeleteItem(&dynamodb.DeleteItemInput{
			TableName:           aws.String(tableName),
			Key:                 item,
			ConditionExpression: aws.String(filterExpression),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":cutoff": {N: &cutoff},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

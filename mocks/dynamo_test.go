// Copyright (c) 2016 Twitch Interactive

package mocks

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func TestMockDynamo(t *testing.T) {
	var table = "users"

	mock := NewMockDynamo([]string{table})

	// Make a few objects storable in dynamo
	type user struct {
		Name string
		ID   int64
	}

	ken := user{
		Name: "Ken Thompson",
		ID:   1,
	}
	user1, err := dynamodbattribute.MarshalMap(ken)
	if err != nil {
		t.Fatalf("MarshalMap(user1) err=%q", err)
	}
	rob := user{
		Name: "Rob Pike",
		ID:   2,
	}
	user2, err := dynamodbattribute.MarshalMap(rob)
	if err != nil {
		t.Fatalf("MarshalMap(user2) err=%q", err)
	}

	// Put the objects in
	if _, err = mock.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      user1,
	}); err != nil {
		t.Errorf("PutItem(user1) err=%q", err)
	}

	if _, err = mock.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      user2,
	}); err != nil {
		t.Errorf("PutItem(user2) err=%q", err)
	}

	// Try putting one into a nonexistent table - this should error
	if _, err = mock.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String("nonexistent table"),
		Item:      user1,
	}); err == nil {
		t.Errorf("Writing to a nonexistent table should error")
	}

	// Get user1 back out
	resp, err := mock.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {N: aws.String("1")},
		},
	})
	if err != nil {
		t.Errorf("GetItem(key1) err=%q", err)
	}

	var returnedUser user
	if err = dynamodbattribute.UnmarshalMap(resp.Item, &returnedUser); err != nil {
		t.Fatalf("UnmarshalMap(GetItem response) err=%q", err)
	}

	if !reflect.DeepEqual(ken, returnedUser) {
		t.Errorf("Unexpected response from GetItem call. have=%+v  want=%+v", returnedUser, ken)
	}

	// Scan users with a filter
	scanInput := &dynamodb.ScanInput{
		TableName:        aws.String(table),
		FilterExpression: aws.String("ID > :id"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id": {N: aws.String("1")},
		},
	}

	var result []user
	err = mock.ScanPages(scanInput, func(page *dynamodb.ScanOutput, last bool) bool {
		for _, item := range page.Items {
			var u user
			err = dynamodbattribute.UnmarshalMap(item, &u)
			if err != nil {
				t.Fatalf("UnmarshalMap(Scan response) err=%q", err)
			}
			result = append(result, u)
		}
		return !last
	})
	if err != nil {
		t.Errorf("ScanPages err=%q", err)
	}

	if len(result) == 1 {
		if !reflect.DeepEqual(result[0], rob) {
			t.Errorf("Unexpected result in scan response. have=%+v  want=%+v", result[0], rob)
		}
	} else {
		t.Errorf("Unexpected number of results from scan, have=%d  want=%d", len(result), 1)
	}

}

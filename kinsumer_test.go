// Copyright (c) 2016 Twitch Interactive
package kinsumer

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	awsRegion             = flag.String("region", "us-west-2", "Region to run tests in")
	dynamoEndpoint        = flag.String("dynamo_endpoint", "http://localhost:4567", "Endpoint for dynamo test server")
	kinesisEndpoint       = flag.String("kinesis_endpoint", "http://localhost:4568", "Endpoint for kinesis test server")
	resourceChangeTimeout = flag.Duration("resource_change_timeout", 50*time.Millisecond, "Timeout between changes to the resource infrastructure")
	streamName            = flag.String("stream_name", "kinsumer_test", "Name of kinesis stream to use for tests")
	applicationName       = flag.String("application_name", "kinsumer_test", "Name of the application, will impact dynamo table names")
	dynamoSuffixes        = []string{"_checkpoints", "_clients", "_metadata"}
	dynamoKeys            = map[string]string{"_checkpoints": "Shard", "_clients": "ID", "_metadata": "Key"}
)

const (
	shardCount int64 = 10
	shardLimit int64 = 100
)

func TestNewWithInterfaces(t *testing.T) {
	s := session.Must(session.NewSession())
	k := kinesis.New(s)
	d := dynamodb.New(s)

	// No kinesis
	_, err := NewWithInterfaces(nil, d, "stream", "app", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// No dynamodb
	_, err = NewWithInterfaces(k, nil, "stream", "app", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// No streamName
	_, err = NewWithInterfaces(k, d, "", "app", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// No applicationName
	_, err = NewWithInterfaces(k, d, "stream", "", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// Invalid config
	_, err = NewWithInterfaces(k, d, "stream", "app", "client", Config{})
	assert.NotEqual(t, err, nil)

	// All ok
	kinsumer, err := NewWithInterfaces(k, d, "stream", "app", "client", NewConfig())
	assert.Equal(t, err, nil)
	assert.NotEqual(t, kinsumer, nil)
}

func CreateFreshStream(t *testing.T, k kinesisiface.KinesisAPI) error {
	_, err := k.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: streamName,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() != "ResourceNotFoundException" {
				return err
			}
		}
	} else {
		// Wait for the stream to be deleted
		time.Sleep(*resourceChangeTimeout)
	}

	_, err = k.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(shardCount),
		StreamName: streamName,
	})

	if err != nil {
		return err
	}
	time.Sleep(*resourceChangeTimeout)

	return nil
}

func CreateFreshTable(t *testing.T, d dynamodbiface.DynamoDBAPI, tableName string, keyName string) error {
	_, err := d.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() != "ResourceNotFoundException" {
				return err
			}
		}
	} else {
		// Wait for table to be deleted
		time.Sleep(*resourceChangeTimeout)
	}

	_, err = d.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(keyName),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(keyName),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(tableName),
	})

	if err != nil {
		return err
	}
	time.Sleep(*resourceChangeTimeout)

	return nil
}

func SetupTestEnvironment(t *testing.T, k kinesisiface.KinesisAPI, d dynamodbiface.DynamoDBAPI) error {
	err := CreateFreshStream(t, k)
	if err != nil {
		return fmt.Errorf("Error creating fresh stream: %s", err)
	}

	for _, s := range dynamoSuffixes {
		err = CreateFreshTable(t, d, *applicationName+s, dynamoKeys[s])
		if err != nil {
			return fmt.Errorf("Error creating fresh %s table: %s", s, err)
		}
	}

	time.Sleep(*resourceChangeTimeout)
	return nil
}

func ignoreResourceNotFound(err error) error {
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() != "ResourceNotFoundException" {
				return err
			}
		}
	} else {
		time.Sleep(*resourceChangeTimeout)
	}

	return nil
}

func CleanupTestEnvironment(t *testing.T, k kinesisiface.KinesisAPI, d dynamodbiface.DynamoDBAPI) error {
	_, err := k.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: streamName,
	})

	if e := ignoreResourceNotFound(err); e != nil {
		return fmt.Errorf("Error deleting kinesis stream: %s", e)
	}

	for _, s := range dynamoSuffixes {
		_, err = d.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(*applicationName + s),
		})

		if e := ignoreResourceNotFound(err); e != nil {
			return fmt.Errorf("Error deleting %s table: %s", s, err)
		}
	}

	return nil
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func SpamStream(t *testing.T, k kinesisiface.KinesisAPI, numEvents int64) error {
	var (
		records []*kinesis.PutRecordsRequestEntry
		counter int64
	)

	for counter = 0; counter < numEvents; counter++ {
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         []byte(strconv.FormatInt(counter, 10)),
			PartitionKey: aws.String(randStringBytes(10)),
		})

		if len(records) == 100 {
			pro, err := k.PutRecords(&kinesis.PutRecordsInput{
				StreamName: streamName,
				Records:    records,
			})

			if err != nil {
				return fmt.Errorf("Error putting records onto stream: %s", err)
			}
			failed := aws.Int64Value(pro.FailedRecordCount)
			require.EqualValues(t, 0, failed)
			records = nil
		}
	}
	if len(records) > 0 {
		pro, err := k.PutRecords(&kinesis.PutRecordsInput{
			StreamName: streamName,
			Records:    records,
		})
		if err != nil {
			return fmt.Errorf("Error putting records onto stream: %s", err)
		}
		failed := aws.Int64Value(pro.FailedRecordCount)
		require.EqualValues(t, 0, failed)
	}

	return nil
}

func KinesisAndDynamoInstances() (kinesisiface.KinesisAPI, dynamodbiface.DynamoDBAPI) {
	kc := aws.NewConfig().WithRegion(*awsRegion).WithLogLevel(3)
	if len(*kinesisEndpoint) > 0 {
		kc = kc.WithEndpoint(*kinesisEndpoint)
	}

	dc := aws.NewConfig().WithRegion(*awsRegion).WithLogLevel(3)
	if len(*dynamoEndpoint) > 0 {
		dc = dc.WithEndpoint(*dynamoEndpoint)
	}

	k := kinesis.New(session.Must(session.NewSession(kc)))
	d := dynamodb.New(session.Must(session.NewSession(dc)))

	return k, d
}

func TestSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := KinesisAndDynamoInstances()

	defer func() {
		err := CleanupTestEnvironment(t, k, d)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := SetupTestEnvironment(t, k, d)
	require.NoError(t, err, "Problems setting up the test environment")

	err = SpamStream(t, k, 233)
	require.NoError(t, err, "Problems spamming stream with events")

}

// This is not a real final test. It's just a harness for development and to kind of think through the interface
func TestKinsumer(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 3
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := KinesisAndDynamoInstances()

	defer func() {
		err := CleanupTestEnvironment(t, k, d)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := SetupTestEnvironment(t, k, d)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)
	eventsPerClient := make([]int, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, *streamName, *applicationName, fmt.Sprintf("test_%d", i), config)
		require.NoError(t, err, "NewWithInterfaces() failed")

		err = clients[i].Run()
		require.NoError(t, err, "kinsumer.Run() failed")
		err = clients[i].Run()
		assert.Error(t, err, "second time calling kinsumer.Run() should fail")

		waitGroup.Add(1)
		go func(client *Kinsumer, ci int) {
			defer waitGroup.Done()
			for {
				data, innerError := client.Next()
				require.NoError(t, innerError, "kinsumer.Next() failed")
				if data == nil {
					return
				}
				idx, _ := strconv.Atoi(string(data))
				output <- idx
				eventsPerClient[ci]++
			}
		}(clients[i], i)
		defer func(ci int) {
			if clients[ci] != nil {
				clients[ci].Stop()
			}
		}(i)
	}

	err = SpamStream(t, k, numberOfEventsToTest)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	for ci, client := range clients {
		client.Stop()
		clients[ci] = nil
	}

	drain(t, output)

	// Make sure the go routines have finished
	waitGroup.Wait()
}

// TestLeader is an integration test of leadership claiming and deleting old clients.
func TestLeader(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 2
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := KinesisAndDynamoInstances()

	defer func() {
		err := CleanupTestEnvironment(t, k, d)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := SetupTestEnvironment(t, k, d)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	// Put an old client that should be deleted.
	now := time.Now().Add(-time.Hour * 24 * 7)
	item, err := dynamodbattribute.MarshalMap(clientRecord{
		ID:            "Old",
		Name:          "Old",
		LastUpdate:    now.UnixNano(),
		LastUpdateRFC: now.UTC().Format(time.RFC1123Z),
	})
	require.NoError(t, err, "Problems converting old client")

	clientsTableName := aws.String(*streamName + "_clients")
	_, err = d.PutItem(&dynamodb.PutItemInput{
		TableName: clientsTableName,
		Item:      item,
	})
	require.NoError(t, err, "Problems putting old client")

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, *streamName, *applicationName, fmt.Sprintf("test_%d", i), config)
		require.NoError(t, err, "NewWithInterfaces() failed")
		clients[i].clientID = strconv.Itoa(i + 1)

		err = clients[i].Run()
		require.NoError(t, err, "kinsumer.Run() failed")

		waitGroup.Add(1)
		go func(client *Kinsumer, ci int) {
			defer waitGroup.Done()
			for {
				data, innerError := client.Next()
				require.NoError(t, innerError, "kinsumer.Next() failed")
				if data == nil {
					return
				}
				idx, _ := strconv.Atoi(string(data))
				output <- idx
			}
		}(clients[i], i)
		defer func(ci int) {
			if clients[ci] != nil {
				clients[ci].Stop()
			}
		}(i)
	}

	err = SpamStream(t, k, numberOfEventsToTest)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	resp, err := d.GetItem(&dynamodb.GetItemInput{
		TableName:      clientsTableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {S: aws.String("Old")},
		},
	})
	require.NoError(t, err, "Problem getting old client")
	require.Equal(t, 0, len(resp.Item), "Old client was not deleted")

	assert.Equal(t, true, clients[0].isLeader, "First client is not leader")
	assert.Equal(t, false, clients[1].isLeader, "Second leader is also leader")

	c, err := NewWithInterfaces(k, d, *streamName, *applicationName, fmt.Sprintf("_test_%d", numberOfClients), config)
	require.NoError(t, err, "NewWithInterfaces() failed")
	c.clientID = "0"
	err = c.Run()
	require.NoError(t, err, "kinsumer.Run() failed")
	require.Equal(t, true, c.isLeader, "New client is not leader")
	_, err = clients[0].refreshShards()
	require.NoError(t, err, "Problem refreshing shards of original leader")
	require.Equal(t, false, clients[0].isLeader, "Original leader is still leader")
	c.Stop()

	for ci, client := range clients {
		client.Stop()
		clients[ci] = nil
	}

	drain(t, output)
	// Make sure the go routines have finished
	waitGroup.Wait()
}

// TestSplit is an integration test of merging shards, checking the closed and new shards are handled correctly.
func TestSplit(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 3
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := KinesisAndDynamoInstances()

	defer func() {
		err := CleanupTestEnvironment(t, k, d)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := SetupTestEnvironment(t, k, d)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(50 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, *streamName, *applicationName, fmt.Sprintf("test_%d", i), config)
		require.NoError(t, err, "NewWithInterfaces() failed")
		clients[i].clientID = strconv.Itoa(i + 1)

		err = clients[i].Run()
		require.NoError(t, err, "kinsumer.Run() failed")

		waitGroup.Add(1)
		go func(client *Kinsumer, ci int) {
			defer waitGroup.Done()
			for {
				data, innerError := client.Next()
				require.NoError(t, innerError, "kinsumer.Next() failed")
				if data == nil {
					return
				}
				idx, _ := strconv.Atoi(string(data))
				output <- idx
			}
		}(clients[i], i)
		defer func(ci int) {
			if clients[ci] != nil {
				clients[ci].Stop()
			}
		}(i)
	}

	err = SpamStream(t, k, numberOfEventsToTest)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: streamName,
		Limit:      aws.Int64(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shards := desc.StreamDescription.Shards
	shardMap := make(map[string]*kinesis.Shard)
	for _, shard := range shards {
		shardMap[*shard.ShardId] = shard
	}

	require.True(t, len(shards) >= 2, "Fewer than 2 shards")

	_, err = k.MergeShards(&kinesis.MergeShardsInput{
		StreamName:           streamName,
		ShardToMerge:         aws.String(*shards[0].ShardId),
		AdjacentShardToMerge: aws.String(*shards[1].ShardId),
	})
	require.NoError(t, err, "Problem merging shards")

	require.True(t, shardCount <= shardLimit, "Too many shards")
	timeout := time.After(time.Second)
	for {
		desc, err = k.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: streamName,
			Limit:      aws.Int64(shardLimit),
		})
		require.NoError(t, err, "Error describing stream")
		if *desc.StreamDescription.StreamStatus == "ACTIVE" {
			break
		}
		select {
		case <-timeout:
			require.FailNow(t, "Timedout after merging shards")
		default:
			time.Sleep(*resourceChangeTimeout)
		}
	}
	newShards := desc.StreamDescription.Shards
	require.Equal(t, shardCount+1, int64(len(newShards)), "Wrong number of shards after merging")

	err = SpamStream(t, k, numberOfEventsToTest)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	// Validate finished shards are no longer in the cache
	var expectedShards []string
	for _, shard := range newShards {
		if *shard.ShardId != *shards[0].ShardId && *shard.ShardId != *shards[1].ShardId {
			expectedShards = append(expectedShards, *shard.ShardId)
		}
	}
	sort.Strings(expectedShards)
	cachedShards, err := loadShardIDsFromDynamo(d, clients[0].metadataTableName)
	require.NoError(t, err, "Error loading cached shard IDs")
	require.Equal(t, expectedShards, cachedShards, "Finished shards are still in the cache")

	for ci, client := range clients {
		client.Stop()
		clients[ci] = nil
	}

	drain(t, output)
	// Make sure the go routines have finished
	waitGroup.Wait()
}

func drain(t *testing.T, output chan int) {
	extraEvents := 0
	// Drain in case events duplicated, so we don't hang.
DrainLoop:
	for {
		select {
		case <-output:
			extraEvents++
		default:
			break DrainLoop
		}
	}
	assert.Equal(t, 0, extraEvents, "Got %d extra events afterwards", extraEvents)
}

func readEvents(t *testing.T, output chan int, numberOfEventsToTest int) {
	eventsFound := make([]bool, numberOfEventsToTest)
	total := 0

ProcessLoop:
	for {
		select {
		case idx := <-output:
			assert.Equal(t, false, eventsFound[idx], "Got duplicate event %d", idx)
			eventsFound[idx] = true
			total++
			if total == numberOfEventsToTest {
				break ProcessLoop
			}
		case <-time.After(3 * time.Second):
			break ProcessLoop
		}
	}

	t.Logf("Got all %d out of %d events\n", total, numberOfEventsToTest)
}

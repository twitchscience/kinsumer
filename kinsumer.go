// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/twinj/uuid"
)

type shardConsumerError struct {
	shardID *string
	err     error
}

type consumedRecord struct {
	record       *kinesis.Record // Record retrieved from kinesis
	checkpointer *checkpointer   // Object that will store the checkpoint back to the database
	retrievedAt  time.Time       // Time the record was retrieved from Kinesis
}

// Kinsumer is a Kinesis Consumer that tries to reduce duplicate reads while allowing for mutliple
// clients each processing multiple shards
type Kinsumer struct {
	kinesis               kinesisiface.KinesisAPI   // interface to the kinesis service
	dynamodb              dynamodbiface.DynamoDBAPI // interface to the dynamodb service
	streamName            string                    // name of the kinesis stream to consume from
	shards                []*kinesis.Shard          // all the shards in the stream, for detecting when the shards change
	stop                  chan struct{}             // channel used to signal to all the go routines that we want to stop consuming
	stoprequest           chan bool                 // channel used internally to signal to the main go routine to stop processing
	records               chan *consumedRecord      // channel for the go routines to put the consumed records on
	output                chan *consumedRecord      // unbuffered channel used to communicate from the main loop to the Next() method
	errors                chan error                // channel used to communicate errors back to the caller
	waitGroup             sync.WaitGroup            // waitGroup to sync the consumers go routines on
	mainWG                sync.WaitGroup            // WaitGroup for the mainLoop
	shardErrors           chan shardConsumerError   // all the errors found by the consumers that were not handled
	clientsTableName      string                    // dynamo table where info about each client is stored
	checkpointTableName   string                    // dynamo table where the checkpoints for each shard are stored
	clientID              string                    // identifier to differentiate between the running clients
	clientName            string                    // display name of the client - used just for debugging
	totalClients          int                       // The number of clients that are currently working on this stream
	thisClient            int                       // The (sorted by name) index of this client in the total list
	config                Config                    // configuration struct
	numberOfRuns          int32                     // Used to atomically make sure we only ever allow one Run() to be called
	maxAgeForClientRecord time.Duration             // Cutoff for records we read from dynamodb before we assume the record is stale

}

// New returns a Kinsumer Interface with default kinesis and dynamodb instances, to be used in ec2 instances to get default auth and config
func New(streamName, applicationName, clientName string, config Config) (*Kinsumer, error) {
	return NewWithSession(session.New(), streamName, applicationName, clientName, config)
}

// NewWithSession should be used if you want to override the Kinesis and Dynamo instances with a non-default aws session
func NewWithSession(session *session.Session, streamName, applicationName, clientName string, config Config) (*Kinsumer, error) {
	k := kinesis.New(session)
	d := dynamodb.New(session)

	return NewWithInterfaces(k, d, streamName, applicationName, clientName, config)
}

// NewWithInterfaces allows you to override the Kinesis and Dynamo instances for mocking or using a local set of servers
func NewWithInterfaces(kinesis kinesisiface.KinesisAPI, dynamodb dynamodbiface.DynamoDBAPI, streamName, applicationName, clientName string, config Config) (*Kinsumer, error) {
	if kinesis == nil {
		return nil, ErrNoKinesisInterface
	}
	if dynamodb == nil {
		return nil, ErrNoDynamoInterface
	}
	if streamName == "" {
		return nil, ErrNoStreamName
	}
	if applicationName == "" {
		return nil, ErrNoApplicationName
	}
	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	consumer := &Kinsumer{
		streamName:            streamName,
		kinesis:               kinesis,
		dynamodb:              dynamodb,
		stoprequest:           make(chan bool),
		records:               make(chan *consumedRecord, config.bufferSize),
		output:                make(chan *consumedRecord),
		errors:                make(chan error, 10),
		shardErrors:           make(chan shardConsumerError, 10),
		checkpointTableName:   applicationName + "_checkpoints",
		clientsTableName:      applicationName + "_clients",
		clientID:              uuid.NewV4().String(),
		clientName:            clientName,
		config:                config,
		maxAgeForClientRecord: config.shardCheckFrequency * 5,
	}
	return consumer, nil
}

//TODO: Write unit test - needs dynamo _and_ kinesis mocking
func (k *Kinsumer) refreshShards() (bool, error) {
	var (
		innerError error
		shards     []*kinesis.Shard
	)

	if err := registerWithClientsTable(k.dynamodb, k.clientID, k.clientName, k.clientsTableName); err != nil {
		return false, err
	}

	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(k.streamName),
	}

	err := k.kinesis.DescribeStreamPages(params, func(page *kinesis.DescribeStreamOutput, _ bool) bool {
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
		return true
	})

	if innerError != nil {
		return false, innerError
	}

	if err != nil {
		return false, err
	}

	//TODO: Move this out of refreshShards and into refreshClients
	clients, err := getClients(k.dynamodb, k.clientID, k.clientsTableName, k.maxAgeForClientRecord)
	totalClients := len(clients)
	thisClient := 0

	found := false
	for i, c := range clients {
		if c.ID == k.clientID {
			thisClient = i
			found = true
			break
		}
	}

	// TODO: It might make sense for a client that detects it is the first client (first in the sorted list)
	//       to delete old references in dynamodb. The algorithm would be to use rescan using the same cutoff but with a
	//       flipped filter, then do a conditional delete (in case the entry was updated)
	if !found {
		return false, ErrThisClientNotInDynamo
	}

	if err != nil {
		return false, err
	}

	changed := (totalClients != k.totalClients) ||
		(thisClient != k.thisClient) ||
		(len(k.shards) != len(shards))

	if !changed {
		for idx := range shards {
			if aws.StringValue(shards[idx].ShardId) != aws.StringValue(k.shards[idx].ShardId) {
				changed = true
				break
			}
		}
	}

	if changed {
		k.shards = shards
	}

	if len(k.shards) == 0 && len(shards) > 0 {
		return false, ErrNoShardsAssigned
	}

	k.thisClient = thisClient
	k.totalClients = totalClients

	return changed, nil
}

// TODO: Can we unit test this at all?
func (k *Kinsumer) startConsumers() {
	k.stop = make(chan struct{})

	for i, shard := range k.shards {
		if (i % k.totalClients) == k.thisClient {
			k.waitGroup.Add(1)
			go k.consume(shard.ShardId)
		}
	}
}

func (k *Kinsumer) stopConsumers() {
	close(k.stop)
	k.waitGroup.Wait()
DrainLoop:
	for {
		select {
		case <-k.records:
		default:
			break DrainLoop
		}
	}
}

func (k *Kinsumer) dynamoTableActive(name string) error {
	out, err := k.dynamodb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("error describing table %s: %v", name, err)
	}
	status := aws.StringValue(out.Table.TableStatus)
	if status != "ACTIVE" {
		return fmt.Errorf("table %s exists but state '%s' is not 'ACTIVE'", name, status)
	}
	return nil
}

func (k *Kinsumer) kinesisStreamReady() error {
	out, err := k.kinesis.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(k.streamName),
	})
	if err != nil {
		return fmt.Errorf("error describing stream %s: %v", k.streamName, err)
	}

	status := aws.StringValue(out.StreamDescription.StreamStatus)
	if status != "ACTIVE" {
		return fmt.Errorf("stream %s exists but state '%s' is not 'ACTIVE'", k.streamName, status)
	}

	return nil
}

// Run the kinesis consumer process. This is a blocking call, use Stop() to force it to return
//TODO: Can we unit test this at all?
func (k *Kinsumer) Run() error {
	if err := k.dynamoTableActive(k.checkpointTableName); err != nil {
		return err
	}
	if err := k.dynamoTableActive(k.clientsTableName); err != nil {
		return err
	}
	if err := k.kinesisStreamReady(); err != nil {
		return err
	}

	allowRun := atomic.CompareAndSwapInt32(&k.numberOfRuns, 0, 1)
	if !allowRun {
		return ErrRunTwice
	}

	if _, err := k.refreshShards(); err != nil {
		return err
	}

	defer deregisterFromClientsTable(k.dynamodb, k.clientID, k.clientsTableName)

	k.mainWG.Add(1)
	go func() {
		defer k.mainWG.Done()

		// We close k.output so that Next() stops, this is also the reason
		// we can't allow Run() to be called after Stop() has happened
		defer close(k.output)

		shardChangeTicker := time.NewTicker(k.config.shardCheckFrequency)
		defer func() {
			shardChangeTicker.Stop()
		}()

		var record *consumedRecord
		k.startConsumers()
		defer k.stopConsumers()

		for {
			var (
				input  chan *consumedRecord
				output chan *consumedRecord
			)

			// We only want to be handing one record from the consumers
			// to the user of kinsumer at a time. We do this by only reading
			// one record off the records queue if we do not already have a
			// record to give away
			if record != nil {
				output = k.output
			} else {
				input = k.records
			}

			select {
			case <-k.stoprequest:
				return
			case record = <-input:
			case output <- record:
				record.checkpointer.update(aws.StringValue(record.record.SequenceNumber))
				record = nil
			case se := <-k.shardErrors:
				log.Printf("ShardError (%s): %s", *se.shardID, se.err)
				k.errors <- se.err
			case <-shardChangeTicker.C:
				changed, err := k.refreshShards()
				if err != nil {
					k.errors <- err
				} else if changed {
					shardChangeTicker.Stop()
					k.stopConsumers()
					record = nil
					k.startConsumers()
					// We create a new shardChangeTicker here so that the time it takes to stop and
					// start the consumers is not included in the wait for the next tick.
					shardChangeTicker = time.NewTicker(k.config.shardCheckFrequency)
				}
			}
		}
	}()

	return nil
}

// Stop the consumption of kinesis events
//TODO: Can we unit test this at all?
func (k *Kinsumer) Stop() {
	k.stoprequest <- true
	k.mainWG.Wait()
}

// Next is a blocking function used to get the next record from the kinesis queue, or errors that
// occurred during the processing of kinesis. It's up to the caller to stop processing by calling 'Stop()'
//
// if err is non nil an error occurred in the system.
// if err is nil and data is nil then kinsumer has been stopped
func (k *Kinsumer) Next() (data []byte, err error) {
	select {
	case err = <-k.errors:
		return nil, err
	case record, ok := <-k.output:
		if ok {
			k.config.stats.EventToClient(*record.record.ApproximateArrivalTimestamp, record.retrievedAt)
			data = record.record.Data
		}
	}

	return data, err
}

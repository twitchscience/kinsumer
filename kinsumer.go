// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
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
	shardID string
	action  string
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
	shardIDs              []string                  // all the shards in the stream, for detecting when the shards change
	stop                  chan struct{}             // channel used to signal to all the go routines that we want to stop consuming
	stoprequest           chan bool                 // channel used internally to signal to the main go routine to stop processing
	records               chan *consumedRecord      // channel for the go routines to put the consumed records on
	output                chan *consumedRecord      // unbuffered channel used to communicate from the main loop to the Next() method
	errors                chan error                // channel used to communicate errors back to the caller
	waitGroup             sync.WaitGroup            // waitGroup to sync the consumers go routines on
	mainWG                sync.WaitGroup            // WaitGroup for the mainLoop
	shardErrors           chan shardConsumerError   // all the errors found by the consumers that were not handled
	clientsTableName      string                    // dynamo table of info about each client
	checkpointTableName   string                    // dynamo table of the checkpoints for each shard
	metadataTableName     string                    // dynamo table of metadata about the leader and shards
	clientID              string                    // identifier to differentiate between the running clients
	clientName            string                    // display name of the client - used just for debugging
	totalClients          int                       // The number of clients that are currently working on this stream
	thisClient            int                       // The (sorted by name) index of this client in the total list
	config                Config                    // configuration struct
	numberOfRuns          int32                     // Used to atomically make sure we only ever allow one Run() to be called
	isLeader              bool                      // Whether this client is the leader
	leaderLost            chan bool                 // Channel that receives an event when the node loses leadership
	leaderWG              sync.WaitGroup            // waitGroup for the leader loop
	maxAgeForClientRecord time.Duration             // Cutoff for client/checkpoint records we read from dynamodb before we assume the record is stale
	maxAgeForLeaderRecord time.Duration             // Cutoff for leader/shard cache records we read from dynamodb before we assume the record is stale
}

// New returns a Kinsumer Interface with default kinesis and dynamodb instances, to be used in ec2 instances to get default auth and config
func New(streamName, applicationName, clientName string, config Config) (*Kinsumer, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return NewWithSession(s, streamName, applicationName, clientName, config)
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
		metadataTableName:     applicationName + "_metadata",
		clientID:              uuid.NewV4().String(),
		clientName:            clientName,
		config:                config,
		maxAgeForClientRecord: config.shardCheckFrequency * 5,
		maxAgeForLeaderRecord: config.leaderActionFrequency * 5,
	}
	return consumer, nil
}

// refreshShards registers our client, refreshes the lists of clients and shards, checks if we
// have become/unbecome the leader, and returns whether the shards/clients changed.
//TODO: Write unit test - needs dynamo _and_ kinesis mocking
func (k *Kinsumer) refreshShards() (bool, error) {
	var shardIDs []string

	if err := registerWithClientsTable(k.dynamodb, k.clientID, k.clientName, k.clientsTableName); err != nil {
		return false, err
	}

	//TODO: Move this out of refreshShards and into refreshClients
	clients, err := getClients(k.dynamodb, k.clientID, k.clientsTableName, k.maxAgeForClientRecord)
	if err != nil {
		return false, err
	}

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

	if !found {
		return false, ErrThisClientNotInDynamo
	}

	if thisClient == 0 && !k.isLeader {
		k.becomeLeader()
	} else if thisClient != 0 && k.isLeader {
		k.unbecomeLeader()
	}

	shardIDs, err = loadShardIDsFromDynamo(k.dynamodb, k.metadataTableName)
	if err != nil {
		return false, err
	}

	changed := (totalClients != k.totalClients) ||
		(thisClient != k.thisClient) ||
		(len(k.shardIDs) != len(shardIDs))

	if !changed {
		for idx := range shardIDs {
			if shardIDs[idx] != k.shardIDs[idx] {
				changed = true
				break
			}
		}
	}

	if changed {
		k.shardIDs = shardIDs
	}

	k.thisClient = thisClient
	k.totalClients = totalClients

	return changed, nil
}

// startConsumers launches a shard consumer for each shard we should own
// TODO: Can we unit test this at all?
func (k *Kinsumer) startConsumers() error {
	k.stop = make(chan struct{})
	assigned := false

	if k.thisClient >= len(k.shardIDs) {
		return nil
	}

	for i, shard := range k.shardIDs {
		if (i % k.totalClients) == k.thisClient {
			k.waitGroup.Add(1)
			assigned = true
			go k.consume(shard)
		}
	}
	if len(k.shardIDs) != 0 && !assigned {
		return ErrNoShardsAssigned
	}
	return nil
}

// stopConsumers stops all our shard consumers
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

// dynamoTableActive returns an error if the given table is not ACTIVE
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

// kinesisStreamReady returns an error if the given stream is not ACTIVE
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

// Run runs the main kinesis consumer process. This is a non-blocking call, use Stop() to force it to return.
// This goroutine is responsible for startin/stopping consumers, aggregating all consumers' records,
// updating checkpointers as records are consumed, and refreshing our shard/client list and leadership
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
		deregErr := deregisterFromClientsTable(k.dynamodb, k.clientID, k.clientsTableName)
		if deregErr != nil {
			return fmt.Errorf("error in kinsumer Run initial refreshShards: (%v); "+
				"error deregistering from clients table: (%v)", err, deregErr)
		}
		return fmt.Errorf("error in kinsumer Run initial refreshShards: %v", err)
	}

	k.mainWG.Add(1)
	go func() {
		defer k.mainWG.Done()

		defer func() {
			// Deregister is a nice to have but clients also time out if they
			// fail to deregister, so ignore error here.
			err := deregisterFromClientsTable(k.dynamodb, k.clientID, k.clientsTableName)
			if err != nil {
				k.errors <- fmt.Errorf("error deregistering client: %s", err)
			}
			if k.isLeader {
				close(k.leaderLost)
				k.leaderLost = nil
				k.isLeader = false
			}
			// Do this outside the k.isLeader check in case k.isLeader was false because
			// we lost leadership but haven't had time to shutdown the goroutine yet.
			k.leaderWG.Wait()
		}()

		// We close k.output so that Next() stops, this is also the reason
		// we can't allow Run() to be called after Stop() has happened
		defer close(k.output)

		shardChangeTicker := time.NewTicker(k.config.shardCheckFrequency)
		defer func() {
			shardChangeTicker.Stop()
		}()

		var record *consumedRecord
		if err := k.startConsumers(); err != nil {
			k.errors <- fmt.Errorf("error starting consumers: %s", err)
		}
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
				k.errors <- fmt.Errorf("shard error (%s) in %s: %s", se.shardID, se.action, se.err)
			case <-shardChangeTicker.C:
				changed, err := k.refreshShards()
				if err != nil {
					k.errors <- fmt.Errorf("error refreshing shards: %s", err)
				} else if changed {
					shardChangeTicker.Stop()
					k.stopConsumers()
					record = nil
					if err := k.startConsumers(); err != nil {
						k.errors <- fmt.Errorf("error restarting consumers: %s", err)
					}
					// We create a new shardChangeTicker here so that the time it takes to stop and
					// start the consumers is not included in the wait for the next tick.
					shardChangeTicker = time.NewTicker(k.config.shardCheckFrequency)
				}
			}
		}
	}()

	return nil
}

// Stop stops the consumption of kinesis events
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

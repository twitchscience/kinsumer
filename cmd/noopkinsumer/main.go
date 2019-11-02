// Copyright (c) 2016 Twitch Interactive

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/twitchscience/kinsumer"
	"github.com/twitchscience/kinsumer/statsd"
)

var (
	statsdHostPort     string
	statsdPrefix       string
	kinesisStreamName  string
	createDynamoTables bool
)

func init() {
	flag.StringVar(&statsdHostPort, "statsdHostPort", "", "host:port of statsd server")
	flag.StringVar(&statsdPrefix, "statsdPrefix", "", "statsd prefix")
	flag.StringVar(&kinesisStreamName, "stream", "", "name of kinesis stream")
	flag.BoolVar(&createDynamoTables, "createTables", false, "create dynamo db tables")
}

var (
	records chan []byte
	wg      sync.WaitGroup
	k       *kinsumer.Kinsumer
)

func initKinsumer() {
	var (
		stats kinsumer.StatReceiver = &kinsumer.NoopStatReceiver{}
		err   error
	)

	if len(kinesisStreamName) == 0 {
		log.Fatalln("stream name commandline parameter is required")
	}

	if len(statsdHostPort) > 0 && len(statsdPrefix) > 0 {
		stats, err = statsd.New(statsdHostPort, statsdPrefix)
		if err != nil {
			log.Fatalf("Error creating statsd object: %v", err)
		}
	}

	config := kinsumer.NewConfig().WithStats(stats)
	session := session.Must(session.NewSession(aws.NewConfig()))

	// kinsumer needs a way to differentiate between running clients, generally you want to use information
	// about the machine it is running on like ip. For this example we'll use a uuid
	name := uuid.New().String()

	k, err = kinsumer.NewWithSession(session, kinesisStreamName, "noopkinsumer", name, config)
	if err != nil {
		log.Fatalf("Error creating kinsumer: %v", err)
	}

	if createDynamoTables {
		err = k.CreateRequiredTables()
		if err != nil {
			log.Fatalf("Error creating kinsumer dynamo db tables: %v", err)
		}
	}
}

func runKinsumer() {
	err := k.Run()
	if err != nil {
		log.Fatalf("kinsumer.Kinsumer.Run() returned error %v", err)
	}
}

func consumeRecords() {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			record, err := k.Next()
			if err != nil {
				log.Fatalf("k.Next returned error %v", err)
			}
			if record != nil {
				records <- record
			} else {
				return
			}
		}
	}()
}

func runLoop() {
	var totalConsumed int64

	t := time.NewTicker(3 * time.Second)
	defer t.Stop()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)

	defer func() {
		log.Println("Total records consumed", totalConsumed)
	}()

	for {
		select {
		case <-sigc:
			return
		case <-records:
			totalConsumed++
		case <-t.C:
			fmt.Printf("Consumed %v\r", totalConsumed)
		}
	}
}

func main() {
	flag.Parse()

	records = make(chan []byte)
	initKinsumer()
	runKinsumer()
	consumeRecords()

	runLoop()

	k.Stop()
	wg.Wait()
}

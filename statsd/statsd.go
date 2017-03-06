// Copyright (c) 2016 Twitch Interactive

package statsd

import (
	"fmt"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

// Statsd is a statreceiver that writes stats to a statsd endpoint
type Statsd struct {
	client statsd.StatSender
}

// New creates a new Statsd statreceiver with a new instance of a cactus statter
func New(addr, prefix string) (*Statsd, error) {
	sd, err := statsd.NewClient(addr, prefix)
	if err != nil {
		return nil, err
	}
	return &Statsd{
		client: sd,
	}, nil
}

// NewWithStatter creates a new statreciever wrapping an existing statter
func NewWithStatter(client statsd.StatSender) *Statsd {
	return &Statsd{
		client: client,
	}
}

// Checkpoint implementation that writes to statsd
func (s *Statsd) Checkpoint() {
	_ = s.client.Inc("kinsumer.checkpoints", 1, 1.0)
}

// EventToClient implementation that writes to statsd metrics about a record
// that was consumed by the client
func (s *Statsd) EventToClient(inserted, retrieved time.Time) {
	now := time.Now()

	_ = s.client.Inc("kinsumer.consumed", 1, 1.0)
	_ = s.client.TimingDuration("kinsumer.in_stream", retrieved.Sub(inserted), 1.0)
	_ = s.client.TimingDuration("kinsumer.end_to_end", now.Sub(inserted), 1.0)
	_ = s.client.TimingDuration("kinsumer.in_kinsumer", now.Sub(retrieved), 1.0)
}

// EventsFromKinesis implementation that writes to statsd metrics about records that
// were retrieved from kinesis
func (s *Statsd) EventsFromKinesis(num int, shardID string, lag time.Duration) {
	_ = s.client.TimingDuration(fmt.Sprintf("kinsumer.%s.lag", shardID), lag, 1.0)
	_ = s.client.Inc(fmt.Sprintf("kinsumer.%s.retrieved", shardID), int64(num), 1.0)
}

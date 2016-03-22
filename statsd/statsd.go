// Copyright (c) 2016 Twitch Interactive

package statsd

import (
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

// Statsd is a statreceiver that writes stats to a statsd endpoint
type Statsd struct {
	Client statsd.Statter
}

// New creates a new Statsd statreceiver
func New(addr, prefix string) (*Statsd, error) {
	sd, err := statsd.NewClient(addr, prefix)
	if err != nil {
		return nil, err
	}
	return &Statsd{
		Client: sd,
	}, nil
}

// Checkpoint implementation that writes to statsd
func (s *Statsd) Checkpoint() {
	_ = s.Client.Inc("kinsumer.checkpoints", 1, 1.0)
}

// EventToClient implementation that writes to statsd metrics about a record
// that was consumed by the Client
func (s *Statsd) EventToClient(inserted, retrieved time.Time) {
	now := time.Now()

	_ = s.Client.Inc("kinsumer.consumed", 1, 1.0)
	_ = s.Client.TimingDuration("kinsumer.in_stream", retrieved.Sub(inserted), 1.0)
	_ = s.Client.TimingDuration("kinsumer.end_to_end", now.Sub(inserted), 1.0)
	_ = s.Client.TimingDuration("kinsumer.in_kinsumer", now.Sub(retrieved), 1.0)
}

// EventsFromKinesis implementation that writes to statsd metrics about records that
// were retrieved from kinesis
func (s *Statsd) EventsFromKinesis(num int, lag time.Duration) {
	_ = s.Client.TimingDuration("kinsumer.lag", lag, 1.0)
	_ = s.Client.Inc("kinsumer.retrieved", int64(num), 1.0)
}

// Copyright (c) 2016 Twitch Interactive.

package kinsumer

import (
	"testing"
	"time"

	"github.com/twitchscience/kinsumer/mocks"
)

func TestCheckpointer(t *testing.T) {
	table := "checkpoints"
	mock := mocks.NewMockDynamo([]string{table})
	stats := &NoopStatReceiver{}

	cp, err := capture("shard", table, mock, "ownerName", "ownerId", 3*time.Minute, stats)

	// Initially, we expect that there is no record, so our new record should have no sequence number
	if err != nil {
		t.Errorf("current 1 err=%q", err)
	}
	if cp == nil {
		t.Errorf("Should always be able to capture the shard if there is no entry in dynamo")
	}
	if cp.sequenceNumber != "" {
		t.Errorf("sequence number should initially be an empty string")
	}

	// Update the sequence number. This shouldn't cause any external request.
	mocks.AssertNoRequestsMade(t, mock.(*mocks.MockDynamo), "update(seq1)", func() {
		cp.update("seq1")
	})

	// Now actually commit.
	mocks.AssertRequestMade(t, mock.(*mocks.MockDynamo), "commit(seq1)", func() {
		if _, err = cp.commit(); err != nil {
			t.Errorf("commit seq1 err=%q", err)
		}
	})

	// Call update, but keep the same sequence number
	cp.update("seq1")

	// Since the sequence number hasn't changed, committing shouldn't make a request.
	mocks.AssertNoRequestsMade(t, mock.(*mocks.MockDynamo), "commit unchanged sequence number", func() {
		if _, err = cp.commit(); err != nil {
			t.Errorf("commit unchanged err=%q", err)
		}
	})

	// Call update again with a new value
	cp.update("seq2")

	// committing should trigger a request
	mocks.AssertRequestMade(t, mock.(*mocks.MockDynamo), "commit(seq2)", func() {
		if _, err = cp.commit(); err != nil {
			t.Errorf("commit seq2 err=%q", err)
		}
	})

	// Call update with a new value twice in a row
	cp.update("seq3")
	cp.update("seq3")

	// This should still trigger an update
	mocks.AssertRequestMade(t, mock.(*mocks.MockDynamo), "commit(seq3)", func() {
		if _, err = cp.commit(); err != nil {
			t.Errorf("commit seq3 err=%q", err)
		}
	})

	// Try to get another checkpointer for this shard, should not succeed but not error
	cp2, err := capture("shard", table, mock, "differentOwner", "differentOwnerId", 3*time.Minute, stats)
	if err != nil {
		t.Errorf("cp2 first attempt err=%q", err)
	}
	if cp2 != nil {
		t.Errorf("Should not be able to steal shard")
	}

	cp.update("lastseq")

	// release should trigger an update
	mocks.AssertRequestMade(t, mock.(*mocks.MockDynamo), "cp.release()", func() {
		if err = cp.release(); err != nil {
			t.Errorf("release err=%q", err)
		}
	})

	//TODO: Test fails because dynamo mock does not handle replacing records in put, need to resolve that
	/*
		// Now that we have released the shard, we should be able to grab it
		cp2, err = newCheckpointer(aws.String("shard"), table, mock, "differentOwner", "differentOwnerId", 3*time.Minute)
		if err != nil {
			t.Errorf("cp2 second attempt err=%q", err)
		}
		if cp2 == nil {
			t.Errorf("The shard should be ours!")
		}

		if cp2.sequenceNumber != "lastseq" {
			t.Errorf("Release should have committed `lastseq` but new checkpointer got %s!", cp2.sequenceNumber)
		}
	*/
}

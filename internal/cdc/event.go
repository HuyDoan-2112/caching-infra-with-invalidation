// Package cdc decodes Debezium change events from the cdc.catalog.items topic.
package cdc

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Event is the part of a Debezium change event the invalidator needs.
type Event struct {
	Op       string    // "c", "u", "d" or "r"
	ID       int64     // primary key of the changed row
	CommitAt time.Time // source.ts_ms: when MySQL committed it (snapshot time for "r")
}

// envelope mirrors only the fields we read; encoding/json ignores the rest
type envelope struct {
	Op     string `json:"op"`
	Before *row   `json:"before"` // pointer: JSON null becomes nil
	After  *row   `json:"after"`
	Source struct {
		TsMs int64 `json:"ts_ms"`
	} `json:"source"`
}

type row struct {
	ID *int64 `json:"id"` // pointer: tells "missing" apart from 0
}

// Parse decodes one Debezium JSON value. Tombstones (nil values) are not
// events: the caller must skip them before calling Parse.
func Parse(value []byte) (Event, error) {
	var env envelope
	if err := json.Unmarshal(value, &env); err != nil {
		return Event{}, fmt.Errorf("decode debezium event: %w", err)
	}

	switch env.Op {
	case "c", "u", "d", "r":
	default:
		return Event{}, fmt.Errorf("unsupported op %q", env.Op)
	}

	r := env.After
	if r == nil {
		r = env.Before // a delete only has the old row
	}
	if r == nil {
		return Event{}, errors.New("event has neither before nor after")
	}
	if r.ID == nil {
		return Event{}, errors.New("row has no id")
	}
	if env.Source.TsMs == 0 {
		return Event{}, errors.New("event has no source.ts_ms")
	}

	return Event{Op: env.Op, ID: *r.ID, CommitAt: time.UnixMilli(env.Source.TsMs)}, nil
}

package loadgen_test

import (
	"context"
	"testing"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/loadgen"
)

func TestScheduleSendsEveryRequestOnTime(t *testing.T) {
	const n = 40 // 200 req/s for 200ms
	late := make([]time.Duration, n)
	finished := make([]bool, n)

	sent, dropped := loadgen.Schedule(context.Background(), 200, 200*time.Millisecond, n,
		func(i int, due time.Time) {
			late[i] = time.Since(due)
			time.Sleep(20 * time.Millisecond) // a slow answer, so requests overlap
			finished[i] = true
		})

	if sent != n || dropped != 0 {
		t.Fatalf("sent=%d dropped=%d, want %d and 0", sent, dropped, n)
	}
	for i := range n {
		if !finished[i] {
			t.Errorf("request %d was still running when Schedule returned", i)
		}
		if late[i] > 50*time.Millisecond {
			t.Errorf("request %d started %v after its due time", i, late[i])
		}
	}
}

func TestScheduleDropsWhenAllSlotsAreTaken(t *testing.T) {
	sent, dropped := loadgen.Schedule(context.Background(), 100, 100*time.Millisecond, 2,
		func(int, time.Time) { time.Sleep(500 * time.Millisecond) })

	if sent != 2 || dropped != 8 {
		t.Fatalf("sent=%d dropped=%d, want 2 and 8", sent, dropped)
	}
}

func TestScheduleStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	sent, dropped := loadgen.Schedule(ctx, 100, 10*time.Second, 10, func(int, time.Time) {})

	if took := time.Since(start); took > time.Second {
		t.Errorf("Schedule took %v after cancel, want it to stop soon", took)
	}
	if sent+dropped >= 1000 {
		t.Errorf("sent=%d dropped=%d: it scheduled the whole run", sent, dropped)
	}
}

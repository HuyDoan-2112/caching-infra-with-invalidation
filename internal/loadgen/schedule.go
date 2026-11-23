package loadgen

import (
	"context"
	"sync"
	"time"
)

// Schedule calls send for request i at its due time, start + i×interval, for
// rate requests per second over duration. It never waits for the system under
// test: when maxInFlight requests are already waiting, request i is dropped and
// counted, not delayed.
func Schedule(ctx context.Context, rate int, duration time.Duration, maxInFlight int,
	send func(i int, due time.Time)) (sent, dropped int) {

	interval := time.Second / time.Duration(rate)
	n := int(duration * time.Duration(rate) / time.Second)
	start := time.Now()
	slots := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup

	for i := range n {
		if ctx.Err() != nil {
			break
		}
		due := start.Add(time.Duration(i) * interval)
		time.Sleep(time.Until(due))

		select {
		case slots <- struct{}{}:
			sent++
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				send(i, due)
			}()
		default:
			dropped++
		}
	}
	wg.Wait()
	return sent, dropped

}

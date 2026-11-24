package loadgen

import (
	"math/rand/v2"
	"sync"
)

// Picker chooses which seeded item a request reads or updates.
type Picker func() int64

// Uniform picks every item equally often.
func Uniform(ids []int64) Picker {
	return func() int64 { return ids[rand.IntN(len(ids))] }
}

// Zipf picks a few items most of the time, like a shop's best sellers: with
// s = 1.1 and 10,000 items, the hottest gets about 15% of the picks. rand.Zipf
// is not safe for use by many goroutines at once, so it has its own pen.
func Zipf(ids []int64, s float64) Picker {
	src := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	z := rand.NewZipf(src, s, 1, uint64(len(ids)-1))
	var mu sync.Mutex
	return func() int64 {
		mu.Lock()
		defer mu.Unlock()
		return ids[z.Uint64()]
	}
}

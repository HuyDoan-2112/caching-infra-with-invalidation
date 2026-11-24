package loadgen

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Kind is what a request turned out to be. Each kind gets its own histogram:
// a cache HIT and a MISS take completely different paths.
type Kind int

// The kinds a request can turn out to be, one histogram each.
const (
	ReadHit Kind = iota
	ReadMiss
	Update
	Create
	numKinds
)

// page is one kind's histogram and its lock (the pen): hdrhistogram is not
// safe for use by many goroutines at once.
type page struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

// Recorder keeps one page per Kind.
type Recorder struct {
	pages  [numKinds]page
	errors [numOps]atomic.Int64
}

// NewRecorder returns a Recorder with empty histograms: microseconds from 1µs
// to 60s, within 0.1%.
func NewRecorder() *Recorder {
	r := &Recorder{}
	for k := range r.pages {
		r.pages[k].hist = hdrhistogram.New(1, 60_000_000, 3)
	}
	return r
}

// Fail counts one failed request of op.
func (r *Recorder) Fail(op Op) {
	r.errors[op].Add(1)
}

// Record adds how long a request of kind k took, counted from its due time.
// It returns an error only for a wait above 60s, which the histogram can't hold.
func (r *Recorder) Record(k Kind, due time.Time) error {
	wait := time.Since(due) // stop the stopwatch before waiting for the pen

	p := &r.pages[k]
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hist.RecordValue(wait.Microseconds())
}

// Count is how many requests of kind k were recorded.
func (r *Recorder) Count(k Kind) int64 {
	p := &r.pages[k]
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hist.TotalCount()
}

// Errors is how many requests of op failed.
func (r *Recorder) Errors(op Op) int64 {
	return r.errors[op].Load()
}

// Percentile is the latency at percentile q (0-100) for kind k, or 0 if
// nothing was recorded.
func (r *Recorder) Percentile(k Kind, q float64) time.Duration {
	p := &r.pages[k]
	p.mu.Lock()
	defer p.mu.Unlock()
	return micros(p.hist.ValueAtQuantile(q))
}

// Merge adds everything from has recorded into r: every latency on every page
// and every error count. A percentile of the merged Recorder is exact for all
// the requests together, unlike an average of percentiles.
func (r *Recorder) Merge(from *Recorder) {
	for k := range r.pages {
		dst, src := &r.pages[k], &from.pages[k]
		dst.mu.Lock()
		src.mu.Lock()
		dst.hist.Merge(src.hist)
		src.mu.Unlock()
		dst.mu.Unlock()
	}
	for op := range r.errors {
		r.errors[op].Add(from.errors[op].Load())
	}

}

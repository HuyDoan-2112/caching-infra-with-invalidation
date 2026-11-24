package loadgen

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

var kindNames = [numKinds]string{ReadHit: "read HIT", ReadMiss: "read MISS", Update: "update", Create: "create"}

// Report writes one row per kind: how many were recorded and their latency
// percentiles, counted from each request's due time. Then the errors per call
// and the cache hit ratio. Call it after Schedule returns, when every request
// has recorded.
func (r *Recorder) Report(w io.Writer, title string) error {
	// tabwriter keeps every row in memory until Flush lines up the columns, so
	// the row writes can't fail: Flush is where a write error shows up.
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "%s\n", title)
	_, _ = fmt.Fprintln(tw, "kind\tcount\tp50\tp90\tp99\tp99.9\tmax")
	for k := range numKinds {
		p := &r.pages[k]
		p.mu.Lock()
		h := p.hist
		if h.TotalCount() == 0 {
			_, _ = fmt.Fprintf(tw, "%s\t0\t-\t-\t-\t-\t-\n", kindNames[k])
		} else {
			_, _ = fmt.Fprintf(tw, "%s\t%d\t%v\t%v\t%v\t%v\t%v\n", kindNames[k], h.TotalCount(),
				micros(h.ValueAtQuantile(50)), micros(h.ValueAtQuantile(90)),
				micros(h.ValueAtQuantile(99)), micros(h.ValueAtQuantile(99.9)), micros(h.Max()))
		}
		p.mu.Unlock()
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	hits, misses := r.Count(ReadHit), r.Count(ReadMiss)
	ratio := "-"
	if hits+misses > 0 {
		ratio = fmt.Sprintf("%.1f%%", 100*float64(hits)/float64(hits+misses))
	}
	_, err := fmt.Fprintf(w, "errors: read %d, update %d, create %d\nhit ratio: %s\n",
		r.Errors(OpRead), r.Errors(OpUpdate), r.Errors(OpCreate), ratio)
	return err
}

// micros turns a histogram value (microseconds) into a Duration, so it prints
// as 312µs or 1.234ms.
func micros(v int64) time.Duration {
	return time.Duration(v) * time.Microsecond
}

package loadgen

import (
	"context"
	"math/rand/v2"
	"net/http"
	"time"
)

// Op is which call a request made. Errors are counted per Op, not per Kind: a
// failed read has no X-Cache header, so it is neither a HIT nor a MISS.
type Op int

// The calls a request can make.
const (
	OpRead Op = iota
	OpUpdate
	OpCreate
	numOps
)

// Mix is the percentage of reads and updates; the rest are creates.
type Mix struct{ Read, Update int }

// NewSend returns the send function Schedule calls: it rolls a dice to pick
// read, update or create by mix, makes the call against a random seeded item,
// and records how long it took from its due time.
func NewSend(ctx context.Context, client *http.Client, readURL, writeURL string,
	pick Picker, mix Mix, rec *Recorder) func(i int, due time.Time) {
	return func(i int, due time.Time) {
		id := pick()

		switch roll := rand.IntN(100); {
		case roll < mix.Read:
			hit, err := readItem(ctx, client, readURL, id)
			if err != nil {
				rec.Fail(OpRead)
				return
			}
			kind := ReadMiss
			if hit {
				kind = ReadHit
			}
			if err := rec.Record(kind, due); err != nil {
				rec.Fail(OpRead)
			}
		case roll < mix.Read+mix.Update:
			if err := updateItem(ctx, client, writeURL, id, int64(i)); err != nil {
				rec.Fail(OpUpdate)
				return
			}
			if err := rec.Record(Update, due); err != nil {
				rec.Fail(OpUpdate)
			}
		default:
			if _, err := createItem(ctx, client, writeURL, i); err != nil {
				rec.Fail(OpCreate)
				return
			}
			if err := rec.Record(Create, due); err != nil {
				rec.Fail(OpCreate)
			}
		}

	}
}

// SplitWarmup sends the first n requests to warm and the rest to send, so the
// cold start gets its own report instead of mixing into the steady state.
func SplitWarmup(n int, warm, send func(i int, due time.Time)) func(i int, due time.Time) {
	return func(i int, due time.Time) {
		if i < n {
			warm(i, due)
			return
		}
		send(i, due)
	}
}

// ByWindow sends request i to sends[(i-first)/perWindow], so each window of
// perWindow requests records into its own Recorder. A request outside the
// windows goes to the nearest one.
func ByWindow(first, perWindow int, sends []func(i int, due time.Time)) func(i int, due time.Time) {
	return func(i int, due time.Time) {
		w := (i - first) / perWindow
		w = max(0, min(w, len(sends)-1))
		sends[w](i, due)
	}
}

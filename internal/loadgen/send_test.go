package loadgen_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/loadgen"
)

var allKinds = []loadgen.Kind{loadgen.ReadHit, loadgen.ReadMiss, loadgen.Update, loadgen.Create}

// fakeAPI answers like the reader and writer: odd ids are cache HITs, even ids
// are MISSes.
func fakeAPI(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.Atoi(r.PathValue("id"))
		if id%2 == 1 {
			w.Header().Set("X-Cache", "HIT")
		} else {
			w.Header().Set("X-Cache", "MISS")
		}
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("PUT /api/v1/items/{id}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("POST /api/v1/items", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id": 99}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestSendRecordsEachKindOnItsOwnPage(t *testing.T) {
	srv := fakeAPI(t)
	cases := []struct {
		name string
		ids  []int64
		mix  loadgen.Mix
		want loadgen.Kind
	}{
		{"read hit", []int64{1}, loadgen.Mix{Read: 100}, loadgen.ReadHit},
		{"read miss", []int64{2}, loadgen.Mix{Read: 100}, loadgen.ReadMiss},
		{"update", []int64{1}, loadgen.Mix{Update: 100}, loadgen.Update},
		{"create", []int64{1}, loadgen.Mix{}, loadgen.Create},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := loadgen.NewRecorder()
			send := loadgen.NewSend(context.Background(), srv.Client(), srv.URL, srv.URL, loadgen.Uniform(c.ids), c.mix, rec)
			for i := range 10 {
				send(i, time.Now())
			}

			for _, k := range allKinds {
				want := int64(0)
				if k == c.want {
					want = 10
				}
				if got := rec.Count(k); got != want {
					t.Errorf("kind %d: %d recorded, want %d", k, got, want)
				}
			}
		})
	}
}

func TestSendCountsFailuresAsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	rec := loadgen.NewRecorder()
	send := loadgen.NewSend(context.Background(), srv.Client(), srv.URL, srv.URL,
		loadgen.Uniform([]int64{1}), loadgen.Mix{Read: 100}, rec)
	for i := range 10 {
		send(i, time.Now())
	}

	if got := rec.Errors(loadgen.OpRead); got != 10 {
		t.Errorf("read errors = %d, want 10", got)
	}
	for _, k := range allKinds {
		if got := rec.Count(k); got != 0 {
			t.Errorf("kind %d: %d recorded, want 0: a failure has no latency", k, got)
		}
	}
}

func TestByWindowSendsEachRequestToItsWindow(t *testing.T) {
	got := make([][]int, 3)
	var sends []func(int, time.Time)
	for w := range 3 {
		sends = append(sends, func(i int, _ time.Time) { got[w] = append(got[w], i) })
	}

	send := loadgen.ByWindow(10, 2, sends) // windows start at request 10, 2 requests each
	for i := 10; i < 17; i++ {
		send(i, time.Now())
	}

	want := [][]int{{10, 11}, {12, 13}, {14, 15, 16}} // 16 is past the last window: goes to the last
	if !reflect.DeepEqual(got, want) {
		t.Errorf("windows got %v, want %v", got, want)
	}
}

//go:build integration

// Integration tests for the item HTTP API against a real MySQL and Redis.
// Run with `make test-integration` (needs `make up` first).

package item_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/config"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/mysqldb"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/redisdb"
)

// db and rdb are shared by every test: one pool each, opened once in TestMain.
var (
	db  *sql.DB
	rdb *redis.Client
)

// testTTL is the reader cache TTL in tests; tests check the key expires within it.
const testTTL = time.Minute

func TestMain(m *testing.M) {
	cfg, err := config.Load("WRITER_PORT", "8080")
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	// Fail loudly: asking for integration tests means MySQL and Redis must be there.
	db, err = mysqldb.Open(context.Background(), cfg.MySQL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need MySQL (run `make up`):", err)
		os.Exit(1)
	}
	rdb, err = redisdb.Open(context.Background(), cfg.Redis)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need Redis (run `make up`):", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = rdb.Close()
	_ = db.Close()
	os.Exit(code)
}

// newMux returns the writer's routes backed by the real database.
func newMux() http.Handler {
	mux := http.NewServeMux()
	item.NewWriteHandler(item.NewStore(db), discardLog()).Register(mux)
	return mux
}

// newReaderMux returns the reader's routes as in production: the Redis cache
// in front of the real database.
func newReaderMux() http.Handler {
	return newReaderMuxWith(rdb, prometheus.NewRegistry())
}

// newReaderMuxWith is newReaderMux with the cache using client c and
// registering its metrics in reg. Each call needs its own reg: registering
// the same metric twice panics.
func newReaderMuxWith(c *redis.Client, reg prometheus.Registerer) http.Handler {
	mux := http.NewServeMux()
	cache := item.NewCache(item.NewStore(db), c, discardLog(), testTTL, item.NewCacheMetrics(reg))
	item.NewReadHandler(cache, discardLog()).Register(mux)
	return mux
}

// cacheKey spells out the reader's Redis key format. It is written out here,
// not imported, because the invalidator depends on it: changing it should
// break a test.
func cacheKey(id int64) string { return "v1:item:" + strconv.FormatInt(id, 10) }

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func itemPath(id int64) string { return "/api/v1/items/" + strconv.FormatInt(id, 10) }

// create POSTs body, requires a 201, and deletes the row and its cache entry
// when the test ends so repeated runs do not pile up data in the dev stack.
func create(t *testing.T, h http.Handler, body string) item.Item {
	t.Helper()
	rec := do(t, h, http.MethodPost, "/api/v1/items", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201; body: %s", rec.Code, rec.Body)
	}
	it := decodeItem(t, rec)
	t.Cleanup(func() {
		_, _ = db.Exec("DELETE FROM items WHERE id = ?", it.ID)
		_ = rdb.Del(context.Background(), cacheKey(it.ID)).Err()
	})
	return it
}

func decodeItem(t *testing.T, rec *httptest.ResponseRecorder) item.Item {
	t.Helper()
	var it item.Item
	if err := json.Unmarshal(rec.Body.Bytes(), &it); err != nil {
		t.Fatalf("decode item: %v; body: %s", err, rec.Body)
	}
	return it
}

type errorResponse struct {
	Error struct {
		Code   string            `json:"code"`
		Fields map[string]string `json:"fields"`
	} `json:"error"`
}

// wantError requires the given status and error code.
func wantError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) errorResponse {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, status, rec.Body)
	}
	var e errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode error body: %v; body: %s", err, rec.Body)
	}
	if e.Error.Code != code {
		t.Fatalf("error code = %q, want %q; body: %s", e.Error.Code, code, rec.Body)
	}
	return e
}

// missingID returns an id that no row has: create a row, then delete it.
func missingID(t *testing.T, h http.Handler) int64 {
	t.Helper()
	it := create(t, h, `{"name":"gone","price_cents":1,"currency":"USD"}`)
	if rec := do(t, h, http.MethodDelete, itemPath(it.ID), ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", rec.Code)
	}
	return it.ID
}

func TestCreate(t *testing.T) {
	h := newMux()

	t.Run("optional fields omitted become null", func(t *testing.T) {
		rec := do(t, h, http.MethodPost, "/api/v1/items", `{"name":"Keyboard","price_cents":12999,"currency":"USD"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201; body: %s", rec.Code, rec.Body)
		}
		it := decodeItem(t, rec)
		t.Cleanup(func() { _, _ = db.Exec("DELETE FROM items WHERE id = ?", it.ID) })

		if got, want := rec.Header().Get("Location"), itemPath(it.ID); got != want {
			t.Errorf("Location = %q, want %q", got, want)
		}
		if got := rec.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		if it.ID <= 0 || it.Name != "Keyboard" || it.PriceCents != 12999 || it.Currency != "USD" {
			t.Errorf("item = %+v, want the posted values and a positive id", it)
		}
		if it.Description != nil || it.Stock != nil {
			t.Errorf("description, stock = %v, %v, want nil, nil", it.Description, it.Stock)
		}
		if it.CreatedAt.IsZero() || !it.UpdatedAt.Equal(it.CreatedAt) {
			t.Errorf("created_at, updated_at = %v, %v, want equal and set by MySQL", it.CreatedAt, it.UpdatedAt)
		}
		// The body comes from a read-back; also check the row really is in the table.
		var name string
		if err := db.QueryRow("SELECT name FROM items WHERE id = ?", it.ID).Scan(&name); err != nil || name != "Keyboard" {
			t.Errorf("row in MySQL: name = %q, err = %v", name, err)
		}
	})

	t.Run("all fields, zeros kept, unicode round-trips", func(t *testing.T) {
		it := create(t, h, `{"name":"Bàn phím cơ","description":"87 phím","price_cents":0,"currency":"VND","stock":0}`)
		if it.Name != "Bàn phím cơ" || it.Description == nil || *it.Description != "87 phím" {
			t.Errorf("name, description = %q, %v; want the posted unicode text", it.Name, it.Description)
		}
		if it.PriceCents != 0 || it.Stock == nil || *it.Stock != 0 {
			t.Errorf("price_cents, stock = %d, %v; want 0 and a non-nil 0", it.PriceCents, it.Stock)
		}
	})
}

func TestCreateRejects(t *testing.T) {
	h := newMux()
	tests := []struct {
		name      string
		body      string
		status    int
		code      string
		wantField string // checked only for validation_failed
	}{
		{"empty body", ``, 400, "invalid_json", ""},
		{"not JSON", `not json`, 400, "invalid_json", ""},
		{"unknown field", `{"id":5,"name":"x","price_cents":1,"currency":"USD"}`, 400, "invalid_json", ""},
		{"wrong type", `{"name":"x","price_cents":"abc","currency":"USD"}`, 400, "invalid_json", ""},
		{"stock overflows int32", `{"name":"x","price_cents":1,"currency":"USD","stock":3000000000}`, 400, "invalid_json", ""},
		{"two objects", `{"name":"x","price_cents":1,"currency":"USD"}{}`, 400, "invalid_json", ""},
		{"body over 64 KB", `{"name":"` + strings.Repeat("a", 70_000) + `"}`, 413, "body_too_large", ""},
		{"invalid field", `{"name":"x","price_cents":1,"currency":"usd"}`, 400, "validation_failed", "currency"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := wantError(t, do(t, h, http.MethodPost, "/api/v1/items", tt.body), tt.status, tt.code)
			if tt.wantField != "" && e.Error.Fields[tt.wantField] == "" {
				t.Errorf("fields = %v, want an error on %q", e.Error.Fields, tt.wantField)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	h := newMux()
	orig := create(t, h, `{"name":"Keyboard","description":"old","price_cents":100,"currency":"USD","stock":5}`)
	path := itemPath(orig.ID)

	t.Run("replaces every field", func(t *testing.T) {
		rec := do(t, h, http.MethodPut, path, `{"name":"Keyboard Pro","description":"new","price_cents":200,"currency":"EUR","stock":7}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
		}
		it := decodeItem(t, rec)
		if it.ID != orig.ID || it.Name != "Keyboard Pro" || *it.Description != "new" ||
			it.PriceCents != 200 || it.Currency != "EUR" || *it.Stock != 7 {
			t.Errorf("item = %+v, want the PUT values on id %d", it, orig.ID)
		}
		if !it.CreatedAt.Equal(orig.CreatedAt) {
			t.Errorf("created_at changed from %v to %v", orig.CreatedAt, it.CreatedAt)
		}
	})

	t.Run("same values again is still 200", func(t *testing.T) {
		// MySQL reports 0 changed rows here; ClientFoundRows makes it 1 matched row, not a 404.
		body := `{"name":"Keyboard Pro","description":"new","price_cents":200,"currency":"EUR","stock":7}`
		if rec := do(t, h, http.MethodPut, path, body); rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
		}
	})

	t.Run("omitted optional fields become null", func(t *testing.T) {
		rec := do(t, h, http.MethodPut, path, `{"name":"Keyboard","price_cents":1,"currency":"USD"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
		}
		if it := decodeItem(t, rec); it.Description != nil || it.Stock != nil {
			t.Errorf("description, stock = %v, %v, want nil, nil", it.Description, it.Stock)
		}
	})
}

func TestUpdateRejects(t *testing.T) {
	h := newMux()
	valid := `{"name":"x","price_cents":1,"currency":"USD"}`
	existing := create(t, h, valid)
	missing := missingID(t, h)

	tests := []struct {
		name   string
		path   string
		body   string
		status int
		code   string
	}{
		{"missing item", itemPath(missing), valid, 404, "not_found"},
		{"id not a number", "/api/v1/items/abc", valid, 400, "invalid_id"},
		{"id zero", "/api/v1/items/0", valid, 400, "invalid_id"},
		{"id negative", "/api/v1/items/-1", valid, 400, "invalid_id"},
		{"id overflows int64", "/api/v1/items/" + strconv.FormatUint(math.MaxUint64, 10), valid, 400, "invalid_id"},
		{"id checked before body", "/api/v1/items/abc", `junk`, 400, "invalid_id"},
		{"invalid body", itemPath(existing.ID), `{"name":"x"}`, 400, "validation_failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantError(t, do(t, h, http.MethodPut, tt.path, tt.body), tt.status, tt.code)
		})
	}
}

func TestDelete(t *testing.T) {
	h := newMux()
	it := create(t, h, `{"name":"Temp","price_cents":1,"currency":"USD"}`)
	path := itemPath(it.ID)

	rec := do(t, h, http.MethodDelete, path, "")
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("status = %d, body = %q; want 204 and no body", rec.Code, rec.Body)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM items WHERE id = ?", it.ID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rows with id %d after DELETE = %d, err = %v; want 0", it.ID, n, err)
	}

	wantError(t, do(t, h, http.MethodDelete, path, ""), 404, "not_found")
	wantError(t, do(t, h, http.MethodPut, path, `{"name":"x","price_cents":1,"currency":"USD"}`), 404, "not_found")
}

func TestDeleteRejectsBadID(t *testing.T) {
	h := newMux()
	for _, id := range []string{"abc", "0", "-1"} {
		t.Run(id, func(t *testing.T) {
			wantError(t, do(t, h, http.MethodDelete, "/api/v1/items/"+id, ""), 400, "invalid_id")
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := newMux()
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/items"}, // reads belong to the reader service
		{http.MethodDelete, "/api/v1/items"},
		{http.MethodPost, "/api/v1/items/1"},
	} {
		if rec := do(t, h, tc.method, tc.path, ""); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, want 405", tc.method, tc.path, rec.Code)
		}
	}
}

func TestGet(t *testing.T) {
	writer, reader := newMux(), newReaderMux()

	t.Run("returns what the writer stored", func(t *testing.T) {
		created := create(t, writer, `{"name":"Bàn phím","description":"87 phím","price_cents":0,"currency":"VND","stock":0}`)
		rec := do(t, reader, http.MethodGet, itemPath(created.ID), "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
		}
		if got := rec.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		got := decodeItem(t, rec)
		if got.ID != created.ID || got.Name != created.Name || *got.Description != *created.Description ||
			got.PriceCents != created.PriceCents || got.Currency != created.Currency || *got.Stock != *created.Stock ||
			!got.CreatedAt.Equal(created.CreatedAt) || !got.UpdatedAt.Equal(created.UpdatedAt) {
			t.Errorf("GET = %+v, want %+v", got, created)
		}
	})

	t.Run("null fields stay null", func(t *testing.T) {
		created := create(t, writer, `{"name":"x","price_cents":1,"currency":"USD"}`)
		got := decodeItem(t, do(t, reader, http.MethodGet, itemPath(created.ID), ""))
		if got.Description != nil || got.Stock != nil {
			t.Errorf("description, stock = %v, %v, want nil, nil", got.Description, got.Stock)
		}
	})

	t.Run("sees the latest write", func(t *testing.T) {
		created := create(t, writer, `{"name":"before","price_cents":1,"currency":"USD"}`)
		if rec := do(t, writer, http.MethodPut, itemPath(created.ID), `{"name":"after","price_cents":2,"currency":"USD"}`); rec.Code != http.StatusOK {
			t.Fatalf("PUT status = %d, want 200", rec.Code)
		}
		if got := decodeItem(t, do(t, reader, http.MethodGet, itemPath(created.ID), "")); got.Name != "after" || got.PriceCents != 2 {
			t.Errorf("GET after PUT = %+v, want name \"after\", price 2", got)
		}
	})
}

func TestGetRejects(t *testing.T) {
	writer, reader := newMux(), newReaderMux()
	missing := missingID(t, writer)

	tests := []struct {
		name   string
		path   string
		status int
		code   string
	}{
		{"missing item", itemPath(missing), 404, "not_found"},
		{"id not a number", "/api/v1/items/abc", 400, "invalid_id"},
		{"id zero", "/api/v1/items/0", 400, "invalid_id"},
		{"id negative", "/api/v1/items/-1", 400, "invalid_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantError(t, do(t, reader, http.MethodGet, tt.path, ""), tt.status, tt.code)
		})
	}
}

// wantCache requires a 200 with the given X-Cache value and returns the item.
func wantCache(t *testing.T, rec *httptest.ResponseRecorder, want string) item.Item {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("X-Cache"); got != want {
		t.Fatalf("X-Cache = %q, want %q", got, want)
	}
	return decodeItem(t, rec)
}

func TestGetCache(t *testing.T) {
	writer := newMux()
	ctx := context.Background()

	// newReader returns a reader with its own metrics registry, so each subtest
	// sees only its own counts.
	newReader := func(c *redis.Client) (http.Handler, *prometheus.Registry) {
		reg := prometheus.NewRegistry()
		return newReaderMuxWith(c, reg), reg
	}

	t.Run("miss stores the item, then hits", func(t *testing.T) {
		reader, reg := newReader(rdb)
		created := create(t, writer, `{"name":"cached","price_cents":5,"currency":"USD"}`)
		path := itemPath(created.ID)

		miss := do(t, reader, http.MethodGet, path, "")
		wantCache(t, miss, "MISS")
		ttl, err := rdb.TTL(ctx, cacheKey(created.ID)).Result()
		if err != nil || ttl <= 0 || ttl > testTTL {
			t.Fatalf("TTL after miss = %v, err = %v; want in (0, %v]", ttl, err, testTTL)
		}
		hit := do(t, reader, http.MethodGet, path, "")
		wantCache(t, hit, "HIT")
		if hit.Body.String() != miss.Body.String() {
			t.Errorf("HIT body = %s, want the MISS body %s", hit.Body, miss.Body)
		}
		wantCounters(t, reg, requestsMetric, map[string]float64{"hit": 1, "miss": 1, "not_found": 0, "error": 0})
		wantCounters(t, reg, redisErrorsMetric, map[string]float64{"get": 0, "set": 0, "decode": 0})
	})

	t.Run("hit does not read MySQL", func(t *testing.T) {
		// Change the row behind the cache's back: nothing invalidates it yet
		// (that is the CDC invalidator's job), so a hit must still serve the
		// cached copy. Proves the hit came from Redis alone.
		reader, reg := newReader(rdb)
		created := create(t, writer, `{"name":"before","price_cents":1,"currency":"USD"}`)
		path := itemPath(created.ID)
		wantCache(t, do(t, reader, http.MethodGet, path, ""), "MISS")
		if _, err := db.Exec("UPDATE items SET name = 'after' WHERE id = ?", created.ID); err != nil {
			t.Fatal(err)
		}
		if got := wantCache(t, do(t, reader, http.MethodGet, path, ""), "HIT"); got.Name != "before" {
			t.Errorf("name = %q, want the cached \"before\"", got.Name)
		}
		wantCounters(t, reg, requestsMetric, map[string]float64{"hit": 1, "miss": 1})
	})

	t.Run("not found is not cached", func(t *testing.T) {
		reader, reg := newReader(rdb)
		id := missingID(t, writer)
		rec := do(t, reader, http.MethodGet, itemPath(id), "")
		wantError(t, rec, http.StatusNotFound, "not_found")
		if got := rec.Header().Get("X-Cache"); got != "" {
			t.Errorf("X-Cache on 404 = %q, want none", got)
		}
		if n, err := rdb.Exists(ctx, cacheKey(id)).Result(); err != nil || n != 0 {
			t.Errorf("key exists = %d, err = %v; want 0", n, err)
		}
		wantCounters(t, reg, requestsMetric, map[string]float64{"hit": 0, "miss": 0, "not_found": 1, "error": 0})
	})

	t.Run("corrupt entry is replaced from MySQL", func(t *testing.T) {
		reader, reg := newReader(rdb)
		created := create(t, writer, `{"name":"fresh","price_cents":1,"currency":"USD"}`)
		if err := rdb.Set(ctx, cacheKey(created.ID), "not json", testTTL).Err(); err != nil {
			t.Fatal(err)
		}
		if got := wantCache(t, do(t, reader, http.MethodGet, itemPath(created.ID), ""), "MISS"); got.Name != "fresh" {
			t.Errorf("name = %q, want \"fresh\" from MySQL", got.Name)
		}
		wantCache(t, do(t, reader, http.MethodGet, itemPath(created.ID), ""), "HIT")
		wantCounters(t, reg, requestsMetric, map[string]float64{"hit": 1, "miss": 1})
		wantCounters(t, reg, redisErrorsMetric, map[string]float64{"get": 0, "set": 0, "decode": 1})
	})

	t.Run("redis down falls back to MySQL", func(t *testing.T) {
		reader, reg := newReader(deadRedis(t))
		created := create(t, writer, `{"name":"no cache","price_cents":1,"currency":"USD"}`)
		for range 2 { // never a HIT: nothing can be stored
			if got := wantCache(t, do(t, reader, http.MethodGet, itemPath(created.ID), ""), "MISS"); got.Name != "no cache" {
				t.Errorf("name = %q, want \"no cache\"", got.Name)
			}
		}
		wantCounters(t, reg, requestsMetric, map[string]float64{"hit": 0, "miss": 2})
		// Both GETs failed; neither request tried a SET.
		wantCounters(t, reg, redisErrorsMetric, map[string]float64{"get": 2, "set": 0, "decode": 0})
	})

	t.Run("failed SET still serves the item", func(t *testing.T) {
		// Redis answers GETs but every SET fails: the reader must still reply
		// 200 from MySQL, count the failure, and leave nothing in Redis.
		hook := &failSetHook{}
		failing := redis.NewClient(rdb.Options())
		failing.AddHook(hook)
		t.Cleanup(func() { _ = failing.Close() })
		reader, reg := newReader(failing)

		created := create(t, writer, `{"name":"unsaved","price_cents":1,"currency":"USD"}`)
		for range 2 { // the SET never lands, so the second read misses too
			if got := wantCache(t, do(t, reader, http.MethodGet, itemPath(created.ID), ""), "MISS"); got.Name != "unsaved" {
				t.Errorf("name = %q, want \"unsaved\"", got.Name)
			}
		}
		if n, err := rdb.Exists(ctx, cacheKey(created.ID)).Result(); err != nil || n != 0 {
			t.Errorf("key exists = %d, err = %v; want 0", n, err)
		}
		wantCounters(t, reg, requestsMetric, map[string]float64{"hit": 0, "miss": 2})
		wantCounters(t, reg, redisErrorsMetric, map[string]float64{"get": 0, "set": 2, "decode": 0})
		// One SET per miss: a second SET would double the Redis writes.
		if hook.sets != 2 {
			t.Errorf("SET commands sent = %d, want 2 (one per miss)", hook.sets)
		}
	})
}

// failSetHook makes every Redis SET fail, leaving other commands untouched,
// and counts how many SETs the client tried to send.
type failSetHook struct{ sets int }

func (*failSetHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failSetHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "set" {
			h.sets++
			return errors.New("injected SET failure")
		}
		return next(ctx, cmd)
	}
}

func (*failSetHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestServicesOnlyServeTheirOwnRoutes pins the read/write split: the reader
// never writes and the writer never serves item reads.
func TestServicesOnlyServeTheirOwnRoutes(t *testing.T) {
	writer, reader := newMux(), newReaderMux()
	for _, tc := range []struct {
		name   string
		h      http.Handler
		method string
		path   string
		want   int
	}{
		{"writer has no GET by id", writer, http.MethodGet, "/api/v1/items/1", http.StatusMethodNotAllowed},
		{"reader has no PUT", reader, http.MethodPut, "/api/v1/items/1", http.StatusMethodNotAllowed},
		{"reader has no DELETE", reader, http.MethodDelete, "/api/v1/items/1", http.StatusMethodNotAllowed},
		{"reader has no POST", reader, http.MethodPost, "/api/v1/items", http.StatusNotFound},
	} {
		if rec := do(t, tc.h, tc.method, tc.path, `{}`); rec.Code != tc.want {
			t.Errorf("%s: %s %s = %d, want %d", tc.name, tc.method, tc.path, rec.Code, tc.want)
		}
	}
}

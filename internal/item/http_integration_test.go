//go:build integration

// Integration tests for the item HTTP API against a real MySQL.
// Run with `make test-integration` (needs `make up` first).

package item_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/config"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/mysqldb"
)

// db is shared by every test: one pool, opened once in TestMain.
var db *sql.DB

func TestMain(m *testing.M) {
	cfg, err := config.Load("WRITER_PORT", "8080")
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	db, err = mysqldb.Open(context.Background(), cfg.MySQL)
	if err != nil {
		// Fail loudly: asking for integration tests means MySQL must be there.
		fmt.Fprintln(os.Stderr, "integration tests need MySQL (run `make up`):", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = db.Close()
	os.Exit(code)
}

// newMux returns the writer's routes backed by the real database.
func newMux() http.Handler {
	mux := http.NewServeMux()
	item.NewHandler(item.NewStore(db), slog.New(slog.NewTextHandler(io.Discard, nil))).Register(mux)
	return mux
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func itemPath(id int64) string { return "/api/v1/items/" + strconv.FormatInt(id, 10) }

// create POSTs body, requires a 201, and deletes the row when the test ends
// so repeated runs do not pile up rows in the dev database.
func create(t *testing.T, h http.Handler, body string) item.Item {
	t.Helper()
	rec := do(t, h, http.MethodPost, "/api/v1/items", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201; body: %s", rec.Code, rec.Body)
	}
	it := decodeItem(t, rec)
	t.Cleanup(func() { _, _ = db.Exec("DELETE FROM items WHERE id = ?", it.ID) })
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

package item

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

// maxBodyBytes caps request bodies; a full item is well under 10 KB.
const maxBodyBytes = 64 << 10

// WriteHandler serves the item write API (the writer service).
type WriteHandler struct {
	store *Store
	log   *slog.Logger
}

// NewWriteHandler returns a WriteHandler that saves items in store and logs
// failures to log.
func NewWriteHandler(store *Store, log *slog.Logger) *WriteHandler {
	return &WriteHandler{store: store, log: log}
}

// Register adds the write routes to mux.
func (h *WriteHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/items", h.create)
	mux.HandleFunc("PUT /api/v1/items/{id}", h.update)
	mux.HandleFunc("DELETE /api/v1/items/{id}", h.delete)
}

// Getter looks up one item by id, returning an error wrapping ErrNotFound if
// there is none. *Store implements it; Cache wraps it.
type Getter interface {
	Get(ctx context.Context, id int64) (*Item, error)
}

// CachedGetter looks up one item by id like Getter, and also reports whether
// it was served from the cache. *Cache implements it.
type CachedGetter interface {
	Get(ctx context.Context, id int64) (it *Item, hit bool, err error)
}

// ReadHandler serves the item read API (the reader service).
type ReadHandler struct {
	items CachedGetter
	log   *slog.Logger
}

// NewReadHandler returns a ReadHandler that looks items up in items and logs
// failures to log.
func NewReadHandler(items CachedGetter, log *slog.Logger) *ReadHandler {
	return &ReadHandler{items: items, log: log}
}

// Register adds the read routes to mux.
func (h *ReadHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/items/{id}", h.get)
}

// get handles GET /api/v1/items/{id}: reply 200 with the item.
func (h *ReadHandler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}

	it, hit, err := h.items.Get(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		// A client mistake, not a server problem: no log.
		writeError(w, http.StatusNotFound, "not_found", "item not found", nil)
		return
	case err != nil:
		h.log.Error("get item", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error", nil)
		return
	}
	if hit {
		w.Header().Set("X-Cache", "HIT")
	} else {
		w.Header().Set("X-Cache", "MISS")
	}

	writeJSON(w, http.StatusOK, it)
}

// errorBody is the JSON shape of every error response:
// {"error": {"code": "...", "message": "...", "fields": {...}}}.
type errorBody struct {
	Error errorDetail `json:"error"`
}

// errorDetail is the inner object of errorBody.
type errorDetail struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"` // only for validation_failed
}

// writeJSON sends v as the JSON response body with the given status.
// Headers must be set before WriteHeader, so Content-Type comes first.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends an error response in the errorBody shape.
func writeError(w http.ResponseWriter, status int, code, message string, fields map[string]string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message, Fields: fields}})
}

// create handles POST /api/v1/items: decode, validate, insert, then reply
// 201 with the stored item and a Location header.
func (h *WriteHandler) create(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput(w, r)
	if !ok {
		return
	}

	// Save. Log the real error, but never send DB details to the client.
	it, err := h.store.Create(r.Context(), in)
	if err != nil {
		h.log.Error("create item", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error", nil)
		return
	}

	// 201 + where the new item lives.
	w.Header().Set("Location", "/api/v1/items/"+strconv.FormatInt(it.ID, 10))
	writeJSON(w, http.StatusCreated, it)
}

// update handles PUT /api/v1/items/{id}: replace every field of the item, then
// reply 200 with the stored item. Fields left out of the body become null.
func (h *WriteHandler) update(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput(w, r)
	if !ok {
		return
	}
	it, err := h.store.Update(r.Context(), id, in)
	switch {
	case errors.Is(err, ErrNotFound):
		// A client mistake, not a server problem: no log.
		writeError(w, http.StatusNotFound, "not_found", "item not found", nil)
		return
	case err != nil:
		h.log.Error("update item", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error", nil)
		return
	}

	writeJSON(w, http.StatusOK, it)
}

// delete handles DELETE /api/v1/items/{id}: remove the item, then reply 204
// with no body.
func (h *WriteHandler) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}

	err := h.store.Delete(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		// A client mistake, not a server problem: no log.
		writeError(w, http.StatusNotFound, "not_found", "item not found", nil)
		return
	case err != nil:
		h.log.Error("delete item", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error", nil)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// decodeInput reads, decodes, and validates the request body as an Input.
// On failure it has already written the error response, and the caller
// just returns.
func decodeInput(w http.ResponseWriter, r *http.Request) (*Input, bool) {
	// Cap the body so a huge request can't eat memory.
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	// Decode. Each kind of failure gets its own status and message.
	var in Input
	if err := dec.Decode(&in); err != nil {
		var tooBig *http.MaxBytesError
		switch {
		case errors.As(err, &tooBig):
			writeError(w, http.StatusRequestEntityTooLarge,
				"body_too_large", "request body must be at most 64 KB", nil)
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest,
				"invalid_json", "request body is empty", nil)
		default:
			writeError(w, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		}
		return nil, false
	}

	// Reject anything after the first JSON object, e.g. `{...}{...}`.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be a single JSON object", nil)
		return nil, false
	}

	// Business rules.
	if errs := in.Validate(); errs != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", "invalid fields", errs)
		return nil, false
	}
	return &in, true
}

// parseID reads the {id} path value. On failure it has already written a 400.
func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	s := r.PathValue("id")
	id, err := strconv.ParseInt(s, 10, 64)

	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_id", "id must be a positive integer", nil)
		return 0, false
	}
	return id, true
}

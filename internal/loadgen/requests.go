package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// readItem reads one item through the reader and reports whether the cache
// had it (X-Cache: HIT).
func readItem(ctx context.Context, client *http.Client, readURL string, id int64) (bool, error) {
	url := fmt.Sprintf("%s/api/v1/items/%d", readURL, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("read item %d: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Read to the end: the latency includes the whole answer, and Go only
	// reuses a connection whose body was fully read.
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return false, fmt.Errorf("read item %d: body: %w", id, err)
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("read item %d: %s", id, resp.Status)
	}
	return resp.Header.Get("X-Cache") == "HIT", nil
}

// updateItem changes one item's price through the writer. The price must
// differ from the current one: MySQL writes no binlog event for an update that
// changes nothing, so the cache would never be invalidated.
func updateItem(ctx context.Context, client *http.Client, writeURL string, id, price int64) error {
	body, err := json.Marshal(map[string]any{
		"name":        fmt.Sprintf("seed-%d", id),
		"price_cents": price,
		"currency":    "USD",
	})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/api/v1/items/%d", writeURL, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("update item %d: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("update item %d: body: %w", id, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("update item %d: %s", id, resp.Status)
	}
	return nil
}

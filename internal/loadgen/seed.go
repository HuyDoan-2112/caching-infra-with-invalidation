// Package loadgen sends open-loop load to the reader and writer and records
// how long each request waited.
package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// createItem creates one item through the writer and returns the id MySQL gave it.
// i only makes the name unique ("seed-42"), so a failure says which item it was.
func createItem(ctx context.Context, client *http.Client, writeURL string, i int) (int64, error) {
	body, err := json.Marshal(map[string]any{
		"name":        fmt.Sprintf("seed-%d", i),
		"price_cents": 1000,
		"currency":    "USD",
	})
	if err != nil {
		return 0, fmt.Errorf("item %d: send: %w", i, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, writeURL+"/api/v1/items", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("item %d: send: %w", i, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("item %d: send: %w", i, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		return 0, fmt.Errorf("item %d: %s", i, resp.Status)
	}

	var out struct {
		ID int64 `json:"id"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("item %d: decode: %w", i, err)
	}

	return out.ID, nil
}

// Seed creates n items through the writer, at most parallel at a time, and
// returns their ids. Any failure fails the whole seed: a run on top of
// missing items would blame the reader for 404s.
func Seed(ctx context.Context, client *http.Client, writeURL string, n, parallel int) ([]int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ids := make([]int64, n)
	var (
		firstErr error
		once     sync.Once
	)

	slots := make(chan struct{}, parallel)

	var wg sync.WaitGroup

	for i := range n {
		if ctx.Err() != nil {
			break
		}
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			id, err := createItem(ctx, client, writeURL, i)
			if err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
			ids[i] = id
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return ids, nil
}

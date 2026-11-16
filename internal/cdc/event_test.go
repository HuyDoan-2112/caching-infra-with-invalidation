package cdc_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/cdc"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantOp  string
		wantID  int64
		wantErr bool
	}{
		{name: "create", input: readTestdata(t, "create.json"), wantOp: "c", wantID: 220},
		{name: "delete", input: readTestdata(t, "delete.json"), wantOp: "d", wantID: 220},
		{name: "update", input: readTestdata(t, "update.json"), wantOp: "u", wantID: 220},
		{name: "snapshot", input: readTestdata(t, "snapshot.json"), wantOp: "r", wantID: 211},
		{name: "not json", input: []byte(`{nope`), wantErr: true},
		{name: "no id", input: []byte(`{"op":"u","after":{"name":"x"},"source":{"ts_ms":1}}`), wantErr: true},
		{name: "both null", input: []byte(`{"op":"d","before":null,"after":null,"source":{"ts_ms":1}}`), wantErr: true},
		{name: "unknown op", input: []byte(`{"op":"t","after":{"id":1},"source":{"ts_ms":1}}`), wantErr: true},
		{name: "no ts_ms", input: []byte(`{"op":"u","after":{"id":1}}`), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cdc.Parse(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Parse() = %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			if got.Op != tt.wantOp || got.ID != tt.wantID {
				t.Errorf("Parse() = op %q id %d, want op %q id %d", got.Op, got.ID, tt.wantOp, tt.wantID)
			}
			if got.CommitAt.IsZero() {
				t.Error("CommitAt is zero, want source.ts_ms")
			}
		})
	}
}

// readTestdata loads a real Debezium message captured from cdc.catalog.items.
func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

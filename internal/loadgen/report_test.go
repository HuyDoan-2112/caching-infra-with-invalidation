package loadgen_test

import (
	"strings"
	"testing"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/loadgen"
)

func TestReportShowsCountsErrorsAndHitRatio(t *testing.T) {
	rec := loadgen.NewRecorder()
	for range 3 {
		_ = rec.Record(loadgen.ReadHit, time.Now())
	}
	_ = rec.Record(loadgen.ReadMiss, time.Now())
	rec.Fail(loadgen.OpUpdate)

	var out strings.Builder
	if err := rec.Report(&out, "measured"); err != nil {
		t.Fatal(err)
	}
	got := out.String()

	for _, want := range []string{
		"read HIT   3",
		"read MISS  1",
		"update     0      -",
		"errors: read 0, update 1, create 0",
		"hit ratio: 75.0%",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report is missing %q\n%s", want, got)
		}
	}
}

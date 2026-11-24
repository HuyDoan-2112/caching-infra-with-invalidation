package loadgen_test

import (
	"testing"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/loadgen"
)

// hottestShare picks n times and returns the share of picks the most picked
// item got.
func hottestShare(pick loadgen.Picker, n int) float64 {
	counts := map[int64]int{}
	most := 0
	for range n {
		id := pick()
		counts[id]++
		most = max(most, counts[id])
	}
	return float64(most) / float64(n)
}

func TestUniformSpreadsAndZipfConcentrates(t *testing.T) {
	ids := make([]int64, 10_000)
	for i := range ids {
		ids[i] = int64(i + 1)
	}

	if u := hottestShare(loadgen.Uniform(ids), 100_000); u > 0.01 {
		t.Errorf("uniform: hottest item got %.2f%% of picks, want under 1%%", 100*u)
	}
	if z := hottestShare(loadgen.Zipf(ids, 1.1), 100_000); z < 0.10 || z > 0.20 {
		t.Errorf("zipf: hottest item got %.2f%% of picks, want about 15%%", 100*z)
	}
}

package kafka

import "testing"

func TestComputeLag(t *testing.T) {
	partIDs := []int{2, 0, 1} // unsorted on purpose
	committed := map[int]int64{
		0: 10,  // 10 behind 15 -> lag 5
		1: -1,  // never committed -> counts as 0 -> lag 20
		2: 100, // caught up -> lag 0
	}
	highWater := map[int]int64{0: 15, 1: 20, 2: 100}

	lags, total := computeLag(partIDs, committed, highWater)

	if total != 25 {
		t.Errorf("total lag = %d, want 25", total)
	}
	if len(lags) != 3 || lags[0].Partition != 0 {
		t.Fatalf("expected sorted by partition, got %+v", lags)
	}
	want := []int64{5, 20, 0}
	for i, w := range want {
		if lags[i].Lag != w {
			t.Errorf("partition %d lag = %d, want %d", lags[i].Partition, lags[i].Lag, w)
		}
	}
	if lags[1].Committed != 0 {
		t.Errorf("uncommitted (-1) should normalize to 0, got %d", lags[1].Committed)
	}
}

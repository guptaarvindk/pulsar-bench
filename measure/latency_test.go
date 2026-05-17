package measure

import (
	"testing"
	"time"
)

func TestRecorder_Empty(t *testing.T) {
	r := &Recorder{}
	s := r.Stats()
	if s.Count != 0 {
		t.Errorf("empty recorder Count = %d, want 0", s.Count)
	}
	if s.P50Ms != 0 || s.P99Ms != 0 {
		t.Errorf("empty recorder should have 0 percentiles")
	}
}

func TestRecorder_SingleSample(t *testing.T) {
	r := &Recorder{}
	r.Record(50 * time.Millisecond)
	s := r.Stats()
	if s.Count != 1 {
		t.Fatalf("Count = %d, want 1", s.Count)
	}
	if s.MinMs != 50 || s.MaxMs != 50 || s.P50Ms != 50 || s.P99Ms != 50 {
		t.Errorf("single 50ms sample: min=%f p50=%f p99=%f max=%f", s.MinMs, s.P50Ms, s.P99Ms, s.MaxMs)
	}
}

func TestRecorder_Percentiles(t *testing.T) {
	r := &Recorder{}
	// Record 100 samples: 1ms, 2ms, ..., 100ms
	for i := 1; i <= 100; i++ {
		r.Record(time.Duration(i) * time.Millisecond)
	}
	s := r.Stats()
	if s.Count != 100 {
		t.Fatalf("Count = %d, want 100", s.Count)
	}
	// p50 ≈ 50ms (linear interpolation of sorted [1..100])
	if s.P50Ms < 49 || s.P50Ms > 51 {
		t.Errorf("P50Ms = %f, want ~50", s.P50Ms)
	}
	// p99 ≈ 99ms
	if s.P99Ms < 98 || s.P99Ms > 100 {
		t.Errorf("P99Ms = %f, want ~99", s.P99Ms)
	}
	if s.MinMs != 1 {
		t.Errorf("MinMs = %f, want 1", s.MinMs)
	}
	if s.MaxMs != 100 {
		t.Errorf("MaxMs = %f, want 100", s.MaxMs)
	}
}

func TestRecorder_StatsWindow(t *testing.T) {
	r := &Recorder{}
	// First batch: 10ms × 10
	for i := 0; i < 10; i++ {
		r.Record(10 * time.Millisecond)
	}
	ws1, off1 := r.StatsWindow(0)
	if ws1.Count != 10 {
		t.Errorf("window 1 Count = %d, want 10", ws1.Count)
	}
	if off1 != 10 {
		t.Errorf("offset after window 1 = %d, want 10", off1)
	}

	// Second batch: 20ms × 5
	for i := 0; i < 5; i++ {
		r.Record(20 * time.Millisecond)
	}
	ws2, off2 := r.StatsWindow(off1)
	if ws2.Count != 5 {
		t.Errorf("window 2 Count = %d, want 5", ws2.Count)
	}
	if off2 != 15 {
		t.Errorf("offset after window 2 = %d, want 15", off2)
	}
	// Window 2 should only contain the 20ms samples
	if ws2.P50Ms < 19 || ws2.P50Ms > 21 {
		t.Errorf("window 2 P50Ms = %f, want ~20", ws2.P50Ms)
	}

	// Empty window (no new samples)
	ws3, off3 := r.StatsWindow(off2)
	if ws3.Count != 0 {
		t.Errorf("empty window Count = %d, want 0", ws3.Count)
	}
	if off3 != 15 {
		t.Errorf("empty window offset = %d, want 15", off3)
	}
}

func TestRecorder_Concurrent(t *testing.T) {
	r := &Recorder{}
	done := make(chan struct{})
	const goroutines = 20
	const samplesEach = 500

	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < samplesEach; j++ {
				r.Record(time.Millisecond)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	s := r.Stats()
	if s.Count != goroutines*samplesEach {
		t.Errorf("Count = %d, want %d", s.Count, goroutines*samplesEach)
	}
}

func TestStallTracker(t *testing.T) {
	st := &StallTracker{}

	// No samples → 0%
	if pct := st.StallPct(); pct != 0 {
		t.Errorf("empty stall pct = %f, want 0", pct)
	}

	// 50% IO, 50% compute → 50% stall
	st.AddIO(100 * time.Millisecond)
	st.AddCompute(100 * time.Millisecond)
	if pct := st.StallPct(); pct < 49 || pct > 51 {
		t.Errorf("50/50 stall pct = %f, want ~50", pct)
	}

	// Only IO, no compute → 0% (metric meaningless without compute gap)
	st2 := &StallTracker{}
	st2.AddIO(100 * time.Millisecond)
	if pct := st2.StallPct(); pct != 0 {
		t.Errorf("IO-only stall pct = %f, want 0 (no compute gap)", pct)
	}

	// nil tracker is safe
	var nilSt *StallTracker
	nilSt.AddIO(time.Millisecond)
	nilSt.AddCompute(time.Millisecond)
	if nilSt.StallPct() != 0 {
		t.Error("nil StallTracker.StallPct() should return 0")
	}
}

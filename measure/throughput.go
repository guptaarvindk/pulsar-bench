package measure

import (
	"sync/atomic"
	"time"
)

// Throughput tracks bytes transferred across all concurrent workers
// using atomic counters so there is zero lock contention on the hot path.
type Throughput struct {
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
	opsRead      atomic.Int64
	opsWrite     atomic.Int64
	start        time.Time
}

func NewThroughput() *Throughput {
	return &Throughput{start: time.Now()}
}

func (t *Throughput) AddRead(bytes int64) {
	t.bytesRead.Add(bytes)
	t.opsRead.Add(1)
}

func (t *Throughput) AddWrite(bytes int64) {
	t.bytesWritten.Add(bytes)
	t.opsWrite.Add(1)
}

// Stats computes final throughput. Call after all workers finish.
func (t *Throughput) Stats(elapsed time.Duration) ThroughputStats {
	if elapsed <= 0 {
		elapsed = time.Since(t.start)
	}
	secs := elapsed.Seconds()
	br := t.bytesRead.Load()
	bw := t.bytesWritten.Load()
	return ThroughputStats{
		ElapsedS:    secs,
		BytesRead:   br,
		BytesWritten: bw,
		ReadGBps:    float64(br) / (1e9 * secs),
		WriteGBps:   float64(bw) / (1e9 * secs),
		ReadMBps:    float64(br) / (1e6 * secs),
		WriteMBps:   float64(bw) / (1e6 * secs),
		ReadOps:     t.opsRead.Load(),
		WriteOps:    t.opsWrite.Load(),
		ReadIOPS:    float64(t.opsRead.Load()) / secs,
		WriteIOPS:   float64(t.opsWrite.Load()) / secs,
	}
}

type ThroughputStats struct {
	ElapsedS     float64
	BytesRead    int64
	BytesWritten int64
	ReadGBps     float64
	WriteGBps    float64
	ReadMBps     float64
	WriteMBps    float64
	ReadOps      int64
	WriteOps     int64
	ReadIOPS     float64
	WriteIOPS    float64
}

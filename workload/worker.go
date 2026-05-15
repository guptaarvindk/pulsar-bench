package workload

import (
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/profile"
)

// worker is the per-goroutine I/O loop.
type worker struct {
	kind     string
	files    []string
	p        *profile.Profile
	id       int
	rng      *rand.Rand
	buf      []byte
}

func newWorker(kind string, files []string, p *profile.Profile, id int, rng *rand.Rand) *worker {
	return &worker{
		kind:  kind,
		files: files,
		p:     p,
		id:    id,
		rng:   rng,
		buf:   make([]byte, p.BlockSize),
	}
}

// Run is the hot loop. It calls the appropriate I/O pattern until ctx is cancelled.
func (w *worker) Run(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) {
	switch w.kind {
	case "sequential-read":
		w.loopSequentialRead(ctx, tp, ttfb, opLat)
	case "random-read":
		w.loopRandomRead(ctx, tp, ttfb, opLat)
	case "write":
		w.loopWrite(ctx, tp, ttfb, opLat)
	case "mixed":
		w.loopMixed(ctx, tp, ttfb, opLat)
	case "agent-workspace":
		w.loopAgentWorkspace(ctx, tp, ttfb, opLat)
	case "multi-epoch":
		w.loopSequentialRead(ctx, tp, ttfb, opLat) // epoch driver in runner
	}
}

// loopSequentialRead reads files sequentially from start to end.
// Each call to a file measures TTFB (time to first block).
func (w *worker) loopSequentialRead(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) {
	idx := w.id % len(w.files) // each worker owns a shard
	for ctx.Err() == nil {
		path := w.files[idx]
		t0 := time.Now()
		f, err := os.Open(path)
		if err != nil {
			continue
		}

		// TTFB = time to first block
		firstRead := true
		for ctx.Err() == nil {
			opStart := time.Now()
			n, err := f.Read(w.buf)
			if n > 0 {
				if firstRead {
					if ttfb != nil {
						ttfb.RecordTTFB(time.Since(t0))
					}
					firstRead = false
				}
				if tp != nil {
					tp.AddRead(int64(n))
				}
				if opLat != nil {
					opLat.Record(time.Since(opStart))
				}
			}
			if err == io.EOF || err != nil {
				break
			}
		}
		f.Close()

		// Advance to next file; wrap around (re-read = warm cache scenario)
		if w.p.Reuse {
			idx = (idx + 1) % len(w.files)
		} else {
			idx = w.rng.Intn(len(w.files))
		}
	}
}

// loopRandomRead seeks to random offsets within files.
// Exercises random-access path — important for mmap-style access.
func (w *worker) loopRandomRead(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) {
	for ctx.Err() == nil {
		path := w.files[w.rng.Intn(len(w.files))]
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		maxOff := st.Size() - w.p.BlockSize
		if maxOff <= 0 {
			maxOff = 0
		}
		offset := w.rng.Int63n(maxOff + 1)

		t0 := time.Now()
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			continue
		}
		opStart := time.Now()
		n, _ := f.Read(w.buf)
		f.Close()

		if n > 0 {
			if ttfb != nil {
				ttfb.RecordTTFB(time.Since(t0))
			}
			if opLat != nil {
				opLat.Record(time.Since(opStart))
			}
			if tp != nil {
				tp.AddRead(int64(n))
			}
		}
	}
}

// loopWrite writes files sequentially and optionally fsyncs.
func (w *worker) loopWrite(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) {
	idx := w.id % len(w.files)
	for ctx.Err() == nil {
		path := w.files[idx]
		t0 := time.Now()
		f, err := os.Create(path)
		if err != nil {
			continue
		}

		remaining := w.p.Files.SizeBytes
		firstWrite := true
		for remaining > 0 && ctx.Err() == nil {
			n := min64(int64(len(w.buf)), remaining)
			opStart := time.Now()
			written, err := f.Write(w.buf[:n])
			if written > 0 {
				if firstWrite {
					if ttfb != nil {
						ttfb.Record(time.Since(t0))
					}
					firstWrite = false
				}
				if tp != nil {
					tp.AddWrite(int64(written))
				}
				if opLat != nil {
					opLat.Record(time.Since(opStart))
				}
				remaining -= int64(written)
			}
			if err != nil {
				break
			}
		}
		if w.p.FsyncOnWrite {
			f.Sync()
		}
		f.Close()
		idx = (idx + 1) % len(w.files)
	}
}

// loopMixed interleaves reads and writes according to ReadPct/WritePct.
func (w *worker) loopMixed(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) {
	for ctx.Err() == nil {
		if w.rng.Intn(100) < w.p.ReadPct {
			w.doSingleRead(tp, ttfb, opLat)
		} else {
			w.doSingleWrite(tp, opLat)
		}
	}
}

// loopAgentWorkspace simulates AI agent file editing patterns:
// stat → open → read (partial) → write (partial) → rename → fsync
func (w *worker) loopAgentWorkspace(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) {
	dir := filepath.Dir(w.files[0])
	for ctx.Err() == nil {
		roll := w.rng.Intn(100)
		switch {
		case roll < 40:
			// stat() — most common agent operation
			path := w.files[w.rng.Intn(len(w.files))]
			t0 := time.Now()
			os.Stat(path)
			if opLat != nil {
				opLat.Record(time.Since(t0))
			}
		case roll < 60:
			// partial read (agent reads a source file)
			w.doSingleRead(tp, ttfb, opLat)
		case roll < 80:
			// partial write + fsync (agent edits a file)
			w.doSingleWrite(tp, opLat)
		case roll < 90:
			// rename (agent saves a temp file atomically)
			src := w.files[w.rng.Intn(len(w.files))]
			dst := filepath.Join(dir, "tmp-rename.bin")
			os.Rename(src, dst)
			os.Rename(dst, src) // restore
		default:
			// readdir (agent scans directory)
			t0 := time.Now()
			f, err := os.Open(dir)
			if err == nil {
				f.ReadDir(-1)
				f.Close()
			}
			if opLat != nil {
				opLat.Record(time.Since(t0))
			}
		}
	}
}

func (w *worker) doSingleRead(
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) {
	path := w.files[w.rng.Intn(len(w.files))]
	t0 := time.Now()
	f, err := os.Open(path)
	if err != nil {
		return
	}
	opStart := time.Now()
	n, _ := f.Read(w.buf)
	f.Close()
	if n > 0 {
		if ttfb != nil {
			ttfb.RecordTTFB(time.Since(t0))
		}
		if opLat != nil {
			opLat.Record(time.Since(opStart))
		}
		if tp != nil {
			tp.AddRead(int64(n))
		}
	}
}

func (w *worker) doSingleWrite(
	tp *measure.Throughput,
	opLat *measure.Recorder,
) {
	path := w.files[w.rng.Intn(len(w.files))]
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	opStart := time.Now()
	n, _ := f.Write(w.buf)
	if w.p.FsyncOnWrite {
		f.Sync()
	}
	f.Close()
	if n > 0 {
		if opLat != nil {
			opLat.Record(time.Since(opStart))
		}
		if tp != nil {
			tp.AddWrite(int64(n))
		}
	}
}

// runMetadataWorker runs stat/readdir ops and records latency.
func runMetadataWorker(
	ctx context.Context,
	files []string,
	rng *rand.Rand,
	statRec *measure.Recorder,
	rdRec *measure.Recorder,
) {
	if len(files) == 0 {
		return
	}
	dir := filepath.Dir(files[0])
	for ctx.Err() == nil {
		if rng.Intn(10) < 8 { // 80% stat
			path := files[rng.Intn(len(files))]
			t0 := time.Now()
			os.Stat(path)
			statRec.Record(time.Since(t0))
		} else { // 20% readdir
			t0 := time.Now()
			f, err := os.Open(dir)
			if err == nil {
				f.ReadDir(-1)
				f.Close()
			}
			rdRec.Record(time.Since(t0))
		}
	}
}

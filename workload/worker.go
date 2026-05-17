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

type worker struct {
	kind  string
	files []string
	p     *profile.Profile
	id    int
	rng   *rand.Rand
	buf   []byte
}

func newWorker(kind string, files []string, p *profile.Profile, id int, rng *rand.Rand) *worker {
	sz := int(alignedBlockSize(p.BlockSize))
	return &worker{
		kind:  kind,
		files: files,
		p:     p,
		id:    id,
		rng:   rng,
		buf:   makeAlignedBuf(sz),
	}
}

func (w *worker) Run(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
	stall *measure.StallTracker,
) {
	switch w.kind {
	case "sequential-read", "multi-epoch":
		w.loopSequentialRead(ctx, tp, ttfb, opLat, stall)
	case "random-read":
		w.loopRandomRead(ctx, tp, ttfb, opLat, stall)
	case "write":
		w.loopWrite(ctx, tp, ttfb, opLat, stall)
	case "mixed":
		w.loopMixed(ctx, tp, ttfb, opLat, stall)
	case "agent-workspace":
		w.loopAgentWorkspace(ctx, tp, ttfb, opLat, stall)
	}
}

// loopSequentialRead — primary training data loading pattern.
// Each worker owns a shard and reads it start-to-end, measures TTFB on
// every open(), then sleeps the compute gap to simulate GPU processing.
func (w *worker) loopSequentialRead(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
	stall *measure.StallTracker,
) {
	idx := w.id % len(w.files)
	for ctx.Err() == nil {
		path := w.files[idx]
		t0 := time.Now()
		f, err := openForRead(path, w.p.DirectIO)
		if err != nil {
			continue
		}
		firstRead := true
		for ctx.Err() == nil {
			opStart := time.Now()
			n, err := f.Read(w.buf)
			ioElapsed := time.Since(opStart)
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
					opLat.Record(ioElapsed)
				}
				if stall != nil {
					stall.AddIO(ioElapsed)
				}
			}
			if err == io.EOF || err != nil {
				break
			}
		}
		f.Close()
		w.computeGap(stall)
		if w.p.Reuse {
			idx = (idx + 1) % len(w.files)
		} else {
			idx = w.rng.Intn(len(w.files))
		}
	}
}

// loopRandomRead — cache thrash pattern.
// Random file, random aligned offset every read — maximum eviction pressure.
func (w *worker) loopRandomRead(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
	stall *measure.StallTracker,
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
		offset := alignedOffset(w.rng.Int63n(maxOff + 1))

		t0 := time.Now()
		f, err := openForRead(path, w.p.DirectIO)
		if err != nil {
			continue
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			continue
		}
		opStart := time.Now()
		n, _ := f.Read(w.buf)
		ioElapsed := time.Since(opStart)
		f.Close()

		if n > 0 {
			if ttfb != nil {
				ttfb.RecordTTFB(time.Since(t0))
			}
			if opLat != nil {
				opLat.Record(ioElapsed)
			}
			if tp != nil {
				tp.AddRead(int64(n))
			}
			if stall != nil {
				stall.AddIO(ioElapsed)
			}
		}
		w.computeGap(stall)
	}
}

// loopWrite — checkpoint save pattern.
// Writes a full file sequentially, fsyncs if configured, then compute gap.
func (w *worker) loopWrite(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
	stall *measure.StallTracker,
) {
	idx := w.id % len(w.files)
	for ctx.Err() == nil {
		path := w.files[idx]
		// Use the actual file size so variable-distribution profiles
		// (image-training, nlp-training, medical-imaging) write the correct
		// number of bytes per file rather than the profile's base SizeBytes.
		fileSize := w.p.Files.SizeBytes
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			fileSize = st.Size()
		}
		t0 := time.Now()
		f, err := openForWrite(path, w.p.DirectIO)
		if err != nil {
			continue
		}
		remaining := fileSize
		firstWrite := true
		for remaining > 0 && ctx.Err() == nil {
			n := min64(int64(len(w.buf)), remaining)
			opStart := time.Now()
			written, err := f.Write(w.buf[:n])
			ioElapsed := time.Since(opStart)
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
					opLat.Record(ioElapsed)
				}
				if stall != nil {
					stall.AddIO(ioElapsed)
				}
				remaining -= int64(written)
			}
			if err != nil {
				break
			}
		}
		if w.p.FsyncOnWrite {
			fsyncStart := time.Now()
			if syncErr := f.Sync(); syncErr != nil {
				f.Close()
				idx = (idx + 1) % len(w.files)
				continue
			}
			if stall != nil {
				stall.AddIO(time.Since(fsyncStart))
			}
		}
		f.Close()
		w.computeGap(stall)
		idx = (idx + 1) % len(w.files)
	}
}

// loopMixed — configurable read/write ratio.
func (w *worker) loopMixed(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
	stall *measure.StallTracker,
) {
	for ctx.Err() == nil {
		var ioElapsed time.Duration
		if w.rng.Intn(100) < w.p.ReadPct {
			ioElapsed = w.doSingleRead(tp, ttfb, opLat)
		} else {
			ioElapsed = w.doSingleWrite(tp, opLat)
		}
		if stall != nil {
			stall.AddIO(ioElapsed)
		}
		w.computeGap(stall)
	}
}

// loopAgentWorkspace — AI coding agent pattern.
// stat (40%) / read (20%) / write (20%) / rename (10%) / readdir (10%)
func (w *worker) loopAgentWorkspace(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
	stall *measure.StallTracker,
) {
	dir := filepath.Dir(w.files[0])
	for ctx.Err() == nil {
		var ioElapsed time.Duration
		roll := w.rng.Intn(100)
		switch {
		case roll < 40:
			path := w.files[w.rng.Intn(len(w.files))]
			t0 := time.Now()
			os.Stat(path)
			ioElapsed = time.Since(t0)
			if opLat != nil {
				opLat.Record(ioElapsed)
			}
		case roll < 60:
			ioElapsed = w.doSingleRead(tp, ttfb, opLat)
		case roll < 80:
			ioElapsed = w.doSingleWrite(tp, opLat)
		case roll < 90:
			src := w.files[w.rng.Intn(len(w.files))]
			dst := filepath.Join(dir, "tmp-rename.bin")
			t0 := time.Now()
			os.Rename(src, dst)
			os.Rename(dst, src)
			ioElapsed = time.Since(t0)
		default:
			t0 := time.Now()
			f, err := os.Open(dir)
			if err == nil {
				f.ReadDir(-1)
				f.Close()
			}
			ioElapsed = time.Since(t0)
			if opLat != nil {
				opLat.Record(ioElapsed)
			}
		}
		if stall != nil {
			stall.AddIO(ioElapsed)
		}
		w.computeGap(stall)
	}
}

// computeGap sleeps to simulate GPU processing time between I/O bursts.
// Records sleep time as productive (non-stall) for GPU stall calculation.
func (w *worker) computeGap(stall *measure.StallTracker) {
	if w.p.ComputeGapMs <= 0 || stall == nil {
		return
	}
	t0 := time.Now()
	time.Sleep(time.Duration(w.p.ComputeGapMs) * time.Millisecond)
	stall.AddCompute(time.Since(t0))
}

func (w *worker) doSingleRead(tp *measure.Throughput, ttfb *measure.Recorder, opLat *measure.Recorder) time.Duration {
	path := w.files[w.rng.Intn(len(w.files))]
	t0 := time.Now()
	f, err := openForRead(path, w.p.DirectIO)
	if err != nil {
		return 0
	}
	opStart := time.Now()
	n, _ := f.Read(w.buf)
	ioElapsed := time.Since(opStart)
	f.Close()
	if n > 0 {
		if ttfb != nil {
			ttfb.RecordTTFB(time.Since(t0))
		}
		if opLat != nil {
			opLat.Record(ioElapsed)
		}
		if tp != nil {
			tp.AddRead(int64(n))
		}
	}
	return ioElapsed
}

func (w *worker) doSingleWrite(tp *measure.Throughput, opLat *measure.Recorder) time.Duration {
	path := w.files[w.rng.Intn(len(w.files))]
	f, err := openForWrite(path, w.p.DirectIO)
	if err != nil {
		return 0
	}
	opStart := time.Now()
	n, _ := f.Write(w.buf)
	if w.p.FsyncOnWrite {
		if syncErr := f.Sync(); syncErr != nil {
			f.Close()
			return 0
		}
	}
	ioElapsed := time.Since(opStart)
	f.Close()
	if n > 0 {
		if opLat != nil {
			opLat.Record(ioElapsed)
		}
		if tp != nil {
			tp.AddWrite(int64(n))
		}
	}
	return ioElapsed
}

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
		if rng.Intn(10) < 8 {
			path := files[rng.Intn(len(files))]
			t0 := time.Now()
			os.Stat(path)
			statRec.Record(time.Since(t0))
		} else {
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

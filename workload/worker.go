package workload

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/profile"
)

type worker struct {
	kind    string
	files   []string
	p       *profile.Profile
	id      int
	rng     *rand.Rand
	rngs    []*rand.Rand // one RNG per iodepth slot — rand.Rand is not goroutine-safe
	buf     []byte       // primary buffer (iodepth slot 0)
	bufs    [][]byte     // one aligned buffer per iodepth slot — no sharing between goroutines
	verify  bool
	iodepth int
}

func newWorker(kind string, files []string, p *profile.Profile, id int, rng *rand.Rand) *worker {
	sz := int(alignedBlockSize(p.BlockSize))
	iodepth := p.IODepth
	if iodepth <= 0 {
		iodepth = 1
	}
	// Pre-allocate one buffer per iodepth slot so concurrent sub-goroutines
	// never share a buffer (which would be a data race).
	bufs := make([][]byte, iodepth)
	for i := range bufs {
		bufs[i] = makeAlignedBuf(sz)
	}
	// One RNG per iodepth slot, seeded from the worker RNG: sub-goroutines
	// launched by runWithIODepth run concurrently and rand.Rand is not
	// goroutine-safe, so they must never share w.rng.
	var rngs []*rand.Rand
	if rng != nil {
		rngs = make([]*rand.Rand, iodepth)
		for i := range rngs {
			rngs[i] = rand.New(rand.NewSource(rng.Int63()))
		}
	}
	return &worker{
		kind:    kind,
		files:   files,
		p:       p,
		id:      id,
		rng:     rng,
		rngs:    rngs,
		buf:     bufs[0],
		bufs:    bufs,
		verify:  p.Verify,
		iodepth: iodepth,
	}
}

// runWithIODepth launches iodepth copies of fn and waits for all to finish.
// Each goroutine receives its own pre-allocated buffer (w.bufs[subID]) so
// concurrent sub-goroutines never race on a shared buffer.
func (w *worker) runWithIODepth(ctx context.Context, fn func(subID int, buf []byte)) {
	if w.iodepth <= 1 {
		fn(0, w.buf)
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < w.iodepth; i++ {
		wg.Add(1)
		i, buf := i, w.bufs[i]
		go func() {
			defer wg.Done()
			fn(i, buf)
		}()
	}
	wg.Wait()
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
	batchTarget := w.p.BatchSizeBytes
	for ctx.Err() == nil {
		w.runWithIODepth(ctx, func(subID int, buf []byte) {
			fileIdx := (w.id*w.iodepth + subID) % len(w.files)
			path := w.files[fileIdx]
			t0 := time.Now()
			f, err := openForRead(path, w.p.DirectIO)
			if err != nil {
				return
			}
			firstRead := true
			var offset int64
			// Accumulate I/O time per simulated training batch. Once a batch's
			// worth of bytes has been read, record that batch's I/O time and run
			// one compute gap (GPU step) — so the stall metric compares per-batch
			// I/O against per-batch compute, the way a DataLoader actually feeds.
			var batchIO time.Duration
			var batchRead int64
			for ctx.Err() == nil {
				opStart := time.Now()
				n, err := f.Read(buf)
				ioElapsed := time.Since(opStart)
				if n > 0 {
					if firstRead {
						if ttfb != nil {
							ttfb.RecordTTFB(time.Since(t0))
						}
						firstRead = false
					}
					if w.verify {
						if verr := verifyCheck(buf[:n], fileIdx, offset); verr != nil {
							fmt.Fprintf(os.Stderr, "verify error: %v\n", verr)
						}
					}
					if tp != nil {
						tp.AddRead(int64(n))
					}
					if opLat != nil {
						opLat.Record(ioElapsed)
					}
					batchIO += ioElapsed
					batchRead += int64(n)
					if batchTarget > 0 && batchRead >= batchTarget {
						if stall != nil {
							stall.AddIO(batchIO)
						}
						w.computeGap(stall)
						batchIO = 0
						batchRead = 0
					}
					offset += int64(n)
				}
				if err == io.EOF || err != nil {
					break
				}
			}
			// Flush a trailing partial batch as one final batch + GPU step.
			if batchRead > 0 {
				if stall != nil {
					stall.AddIO(batchIO)
				}
				w.computeGap(stall)
			}
			f.Close()
		})
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
	// Accumulate I/O time per simulated training batch across many random
	// reads, then run one compute gap (GPU step) per batch — so the stall
	// metric compares per-batch I/O against per-batch compute. Each sub-reader
	// records into its own slot to stay race-free under iodepth.
	batchTarget := w.p.BatchSizeBytes
	var batchIO time.Duration
	var batchRead int64
	subIO := make([]time.Duration, w.iodepth)
	subN := make([]int64, w.iodepth)
	for ctx.Err() == nil {
		w.runWithIODepth(ctx, func(subID int, buf []byte) {
			subIO[subID] = 0
			subN[subID] = 0
			fileIdx := (w.id*w.iodepth + subID) % len(w.files)
			path := w.files[fileIdx]
			st, err := os.Stat(path)
			if err != nil {
				return
			}
			maxOff := st.Size() - w.p.BlockSize
			if maxOff <= 0 {
				maxOff = 0
			}
			// Use the per-slot RNG: with iodepth > 1 this closure runs in
			// concurrent goroutines and sharing w.rng would be a data race.
			offset := alignedOffset(w.rngs[subID].Int63n(maxOff + 1))

			t0 := time.Now()
			f, err := openForRead(path, w.p.DirectIO)
			if err != nil {
				return
			}
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				f.Close()
				return
			}
			opStart := time.Now()
			n, _ := f.Read(buf)
			ioElapsed := time.Since(opStart)
			f.Close()

			if n > 0 {
				if w.verify {
					if verr := verifyCheck(buf[:n], fileIdx, offset); verr != nil {
						fmt.Fprintf(os.Stderr, "verify error: %v\n", verr)
					}
				}
				if ttfb != nil {
					ttfb.RecordTTFB(time.Since(t0))
				}
				if opLat != nil {
					opLat.Record(ioElapsed)
				}
				if tp != nil {
					tp.AddRead(int64(n))
				}
				subIO[subID] = ioElapsed
				subN[subID] = int64(n)
			}
		})
		for i := 0; i < w.iodepth; i++ {
			batchIO += subIO[i]
			batchRead += subN[i]
		}
		if batchTarget > 0 && batchRead >= batchTarget {
			if stall != nil {
				stall.AddIO(batchIO)
			}
			w.computeGap(stall)
			batchIO = 0
			batchRead = 0
		}
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
		var writeOffset int64
		for remaining > 0 && ctx.Err() == nil {
			n := min64(int64(len(w.buf)), remaining)
			if w.verify {
				verifyFill(w.buf[:n], idx, writeOffset)
			}
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
				writeOffset += int64(written)
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
			fsyncElapsed := time.Since(fsyncStart)
			// Include fsync in both opLat and stall to be consistent
			// with doSingleWrite, which measures the whole write+sync block.
			if opLat != nil {
				opLat.Record(fsyncElapsed)
			}
			if stall != nil {
				stall.AddIO(fsyncElapsed)
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
			// Per-worker temp name avoids racing with other workers on a
			// shared "tmp-rename.bin" which would corrupt file contents.
			dst := filepath.Join(dir, fmt.Sprintf("tmp-rename-%d.bin", w.id))
			t0 := time.Now()
			os.Rename(src, dst)
			os.Rename(dst, src)
			ioElapsed = time.Since(t0)
			// Record rename latency like every other branch — leaving it out
			// silently dropped ~10% of ops from the op-latency distribution.
			if opLat != nil {
				opLat.Record(ioElapsed)
			}
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

// verifyFill writes a deterministic pattern into buf starting at startOffset.
//
// Each byte at absolute file position P belongs to the 8-byte chunk whose
// start is P&^7.  The chunk's 8 bytes are derived from a single XorShift64
// keyed on (fileIndex, chunkStart), so the pattern is fully byte-addressable:
// any sub-range of the file can be verified regardless of alignment.
//
// Fast path: when startOffset and len(buf) are both 8-byte aligned the inner
// loop processes 8 bytes per iteration.  The slower per-byte path handles the
// (rare) unaligned case that arises on non-Linux where alignedOffset is a
// no-op.
func verifyFill(buf []byte, fileIndex int, startOffset int64) {
	fi := uint64(fileIndex) * 0x9e3779b97f4a7c15
	abs := uint64(startOffset)

	chunkSeed := func(chunkStart uint64) uint64 {
		s := fi ^ chunkStart*0x6c62272e07bb0142
		s ^= s >> 12
		s ^= s << 25
		s ^= s >> 27
		return s
	}

	if abs&7 == 0 && len(buf)&7 == 0 {
		// Aligned fast path
		for i := 0; i < len(buf); i += 8 {
			seed := chunkSeed(abs + uint64(i))
			buf[i] = byte(seed)
			buf[i+1] = byte(seed >> 8)
			buf[i+2] = byte(seed >> 16)
			buf[i+3] = byte(seed >> 24)
			buf[i+4] = byte(seed >> 32)
			buf[i+5] = byte(seed >> 40)
			buf[i+6] = byte(seed >> 48)
			buf[i+7] = byte(seed >> 56)
		}
		return
	}

	// General path: per-byte (handles any start/length alignment)
	for i := range buf {
		absPos := abs + uint64(i)
		chunkStart := absPos &^ 7
		seed := chunkSeed(chunkStart)
		buf[i] = byte(seed >> ((absPos & 7) * 8))
	}
}

// verifyCheck checks buf against the expected pattern starting at startOffset.
// Returns an error describing the first corrupted byte found.
func verifyCheck(buf []byte, fileIndex int, startOffset int64) error {
	expected := make([]byte, len(buf))
	verifyFill(expected, fileIndex, startOffset)
	for i := range buf {
		if buf[i] != expected[i] {
			return fmt.Errorf("corruption at file %d offset %d+%d: got %02x want %02x",
				fileIndex, startOffset, i, buf[i], expected[i])
		}
	}
	return nil
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

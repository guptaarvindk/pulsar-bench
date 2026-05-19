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
	buf     []byte
	verify  bool
	iodepth int
}

func newWorker(kind string, files []string, p *profile.Profile, id int, rng *rand.Rand) *worker {
	sz := int(alignedBlockSize(p.BlockSize))
	iodepth := p.IODepth
	if iodepth <= 0 {
		iodepth = 1
	}
	return &worker{
		kind:    kind,
		files:   files,
		p:       p,
		id:      id,
		rng:     rng,
		buf:     makeAlignedBuf(sz),
		verify:  p.Verify,
		iodepth: iodepth,
	}
}

// runWithIODepth launches iodepth copies of fn and waits for all to finish.
// Used to simulate higher I/O queue depth within a single worker.
func (w *worker) runWithIODepth(ctx context.Context, fn func(subID int)) {
	if w.iodepth <= 1 {
		fn(0)
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < w.iodepth; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			fn(i)
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
	for ctx.Err() == nil {
		w.runWithIODepth(ctx, func(subID int) {
			fileIdx := (w.id*w.iodepth + subID) % len(w.files)
			path := w.files[fileIdx]
			t0 := time.Now()
			f, err := openForRead(path, w.p.DirectIO)
			if err != nil {
				return
			}
			firstRead := true
			var offset int64
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
					if w.verify {
						if verr := verifyCheck(w.buf[:n], fileIdx, offset); verr != nil {
							fmt.Fprintf(os.Stderr, "verify error: %v\n", verr)
						}
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
					offset += int64(n)
				}
				if err == io.EOF || err != nil {
					break
				}
			}
			f.Close()
		})
		w.computeGap(stall)
		// Advance to next file set after each outer pass
		// (when iodepth==1 this matches the original idx++ behaviour)
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
		w.runWithIODepth(ctx, func(subID int) {
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
			offset := alignedOffset(w.rng.Int63n(maxOff + 1))

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
			n, _ := f.Read(w.buf)
			ioElapsed := time.Since(opStart)
			f.Close()

			if n > 0 {
				if w.verify {
					if verr := verifyCheck(w.buf[:n], fileIdx, offset); verr != nil {
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
				if stall != nil {
					stall.AddIO(ioElapsed)
				}
			}
		})
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

// verifyFill writes a deterministic repeating pattern into buf.
// Pattern is based on fileIndex and blockOffset so each block has a unique value.
func verifyFill(buf []byte, fileIndex int, blockOffset int64) {
	seed := uint64(fileIndex)*0x9e3779b97f4a7c15 + uint64(blockOffset)*0x6c62272e07bb0142
	for i := 0; i+8 <= len(buf); i += 8 {
		seed ^= seed >> 12
		seed ^= seed << 25
		seed ^= seed >> 27
		buf[i] = byte(seed)
		buf[i+1] = byte(seed >> 8)
		buf[i+2] = byte(seed >> 16)
		buf[i+3] = byte(seed >> 24)
		buf[i+4] = byte(seed >> 32)
		buf[i+5] = byte(seed >> 40)
		buf[i+6] = byte(seed >> 48)
		buf[i+7] = byte(seed >> 56)
	}
}

// verifyCheck checks buf against the expected pattern.
// Returns an error if any byte is wrong.
func verifyCheck(buf []byte, fileIndex int, blockOffset int64) error {
	expected := make([]byte, len(buf))
	verifyFill(expected, fileIndex, blockOffset)
	for i := range buf {
		if buf[i] != expected[i] {
			return fmt.Errorf("corruption at file %d offset %d+%d: got %02x want %02x",
				fileIndex, blockOffset, i, buf[i], expected[i])
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

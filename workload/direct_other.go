//go:build !linux

// Fallback for non-Linux platforms (macOS, etc.).
// O_DIRECT is not available; direct=true is silently ignored.
// Benchmarks on non-Linux platforms will hit the OS page cache.

package workload

import "os"

func openForRead(path string, _ bool) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY, 0)
}

func openForWrite(path string, _ bool) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func makeAlignedBuf(size int) []byte {
	return make([]byte, size)
}

func alignedBlockSize(size int64) int64 { return size }

func alignedOffset(off int64) int64 { return off }

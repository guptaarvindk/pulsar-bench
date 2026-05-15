//go:build linux

// Direct I/O helpers for Linux.
// O_DIRECT bypasses the kernel page cache entirely — every read and write
// goes straight to the storage device. This is the only way to get honest
// storage numbers; without it you are benchmarking RAM.
//
// O_DIRECT requirements (enforced here):
//   - Buffer memory address must be aligned to 4096 bytes
//   - Transfer size must be a multiple of 4096 bytes
//   - File offset must be a multiple of 4096 bytes
//
// We satisfy all three: aligned buffers via makeAlignedBuf, sizes rounded
// up to 4096 in blockSize(), and offsets snapped down to 4096 in
// alignedOffset().

package workload

import (
	"os"
	"unsafe"
)

// O_DIRECT on Linux (not in os package constants).
const oDirectFlag = 0x4000

func openForRead(path string, direct bool) (*os.File, error) {
	flags := os.O_RDONLY
	if direct {
		flags |= oDirectFlag
	}
	return os.OpenFile(path, flags, 0)
}

func openForWrite(path string, direct bool) (*os.File, error) {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if direct {
		flags |= oDirectFlag
	}
	return os.OpenFile(path, flags, 0644)
}

// makeAlignedBuf returns a byte slice whose first element is guaranteed to
// be at a 4096-byte-aligned address. The returned length is size rounded up
// to the nearest 4096-byte multiple.
func makeAlignedBuf(size int) []byte {
	const align = 4096
	// Round up to alignment boundary
	size = (size + align - 1) &^ (align - 1)
	// Over-allocate so we can find an aligned start
	raw := make([]byte, size+align)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	off := int((uintptr(align) - addr%uintptr(align)) % uintptr(align))
	// Keep a reference to raw alive so GC does not collect it while buf is in use.
	// buf is a sub-slice of raw; Go guarantees raw stays alive as long as buf is live.
	return raw[off : off+size]
}

// alignedBlockSize rounds size up to the nearest 4096-byte multiple.
// O_DIRECT transfers must be a multiple of the block size.
func alignedBlockSize(size int64) int64 {
	const align = 4096
	return (size + align - 1) &^ (align - 1)
}

// alignedOffset snaps an arbitrary byte offset down to the nearest 4096-byte
// boundary, which is required for O_DIRECT seeks.
func alignedOffset(off int64) int64 {
	const align = 4096
	return off &^ (align - 1)
}

//go:build !linux

package profile

// availableRAMBytes returns a conservative 32 GB default on non-Linux systems.
func availableRAMBytes() int64 {
	return 32 << 30
}

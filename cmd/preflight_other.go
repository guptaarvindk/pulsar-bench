//go:build !linux && !darwin

package cmd

func freeSpaceBytes(path string) (int64, error) {
	return 0, nil // not implemented on this platform
}

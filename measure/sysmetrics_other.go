//go:build !linux

package measure

import "runtime"

func SampleCPUPct() float64 {
	return 0 // not available on macOS without cgo
}

func SampleMemMB() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.Sys) / (1024 * 1024)
}

func SampleDiskIOPS() map[string]float64 {
	return nil // not available on non-Linux
}

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

// NetIfaceStats is defined here for non-Linux builds so the type is always available.
type NetIfaceStats struct {
	RxMBps float64 `json:"rx_mbps"`
	TxMBps float64 `json:"tx_mbps"`
	RxPkts float64 `json:"rx_pkts_s"`
	TxPkts float64 `json:"tx_pkts_s"`
}

func SampleNetStats() map[string]NetIfaceStats {
	return nil // not available on non-Linux
}

//go:build linux

package measure

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cpuState struct {
	user, nice, system, idle, iowait, irq, softirq uint64
}

var prevCPU struct {
	mu    sync.Mutex
	state cpuState
	at    time.Time
}

func readCPULine() (cpuState, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuState{}, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			return cpuState{}, fmt.Errorf("short /proc/stat line")
		}
		var s cpuState
		vals := []*uint64{&s.user, &s.nice, &s.system, &s.idle, &s.iowait, &s.irq, &s.softirq}
		for i, v := range vals {
			n, _ := strconv.ParseUint(fields[i+1], 10, 64)
			*v = n
		}
		return s, nil
	}
	return cpuState{}, fmt.Errorf("cpu line not found")
}

// SampleCPUPct returns the CPU usage as a percentage of total capacity (0–100%).
// This is the system-wide average across all cores, normalised so 100% means
// all cores fully busy. First call always returns 0 (establishes baseline).
func SampleCPUPct() float64 {
	cur, err := readCPULine()
	if err != nil {
		return 0
	}
	prevCPU.mu.Lock()
	prev := prevCPU.state
	prevCPU.state = cur
	prevCPU.mu.Unlock()

	prevTotal := prev.user + prev.nice + prev.system + prev.idle + prev.iowait + prev.irq + prev.softirq
	curTotal := cur.user + cur.nice + cur.system + cur.idle + cur.iowait + cur.irq + cur.softirq
	deltaTotal := float64(curTotal - prevTotal)
	if deltaTotal == 0 {
		return 0
	}
	deltaIdle := float64(cur.idle - prev.idle)
	// /proc/stat reports aggregate jiffies across all CPUs, so dividing
	// (active/total) already yields a 0–100% system-wide average.
	return (1 - deltaIdle/deltaTotal) * 100.0
}

// SampleMemMB returns resident set size of this process in megabytes.
func SampleMemMB() float64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return fallbackMemMB()
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.ParseFloat(fields[1], 64)
				return kb / 1024.0
			}
		}
	}
	return fallbackMemMB()
}

type diskStatEntry struct {
	reads  uint64
	writes uint64
}

var prevDisk struct {
	mu    sync.Mutex
	stats map[string]diskStatEntry
	at    time.Time
}

// SampleDiskIOPS returns map of device name → total IOPS (reads+writes per second) since last call.
func SampleDiskIOPS() map[string]float64 {
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		return nil
	}
	defer f.Close()

	cur := make(map[string]diskStatEntry)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 14 {
			continue
		}
		dev := fields[2]
		// Skip loop, ram, and partition devices (keep only whole disks: sda, nvme0n1, etc.)
		if strings.HasPrefix(dev, "loop") || strings.HasPrefix(dev, "ram") {
			continue
		}
		// Skip partitions (sda1, nvme0n1p1) — keep only whole disks
		if isPartition(dev) {
			continue
		}
		reads, _ := strconv.ParseUint(fields[3], 10, 64)
		writes, _ := strconv.ParseUint(fields[7], 10, 64)
		cur[dev] = diskStatEntry{reads: reads, writes: writes}
	}

	now := time.Now()
	prevDisk.mu.Lock()
	prev := prevDisk.stats
	prevAt := prevDisk.at
	prevDisk.stats = cur
	prevDisk.at = now
	prevDisk.mu.Unlock()

	if prev == nil || prevAt.IsZero() {
		return nil
	}
	elapsed := now.Sub(prevAt).Seconds()
	if elapsed <= 0 {
		return nil
	}

	result := make(map[string]float64)
	for dev, c := range cur {
		if p, ok := prev[dev]; ok {
			deltaR := float64(c.reads - p.reads)
			deltaW := float64(c.writes - p.writes)
			iops := (deltaR + deltaW) / elapsed
			if iops > 0 {
				result[dev] = iops
			}
		}
	}
	return result
}

// isPartition returns true if dev is a partition rather than a whole disk.
//
// Whole-disk patterns we want to KEEP:
//   sda, sdb         — SCSI/SATA whole disks (no trailing digit)
//   nvme0n1          — NVMe whole disk (ends in nN, not nNpM)
//   md0, md127       — Linux software RAID
//   dm-0, dm-1       — Device-mapper (LVM, LUKS, etc.)
//   vda, xvda        — virtio/Xen virtual disks
//
// Partition patterns we want to SKIP:
//   sda1, sdb2       — SCSI/SATA partitions
//   nvme0n1p1        — NVMe partitions (ends in pN)
func isPartition(dev string) bool {
	// NVMe whole disks: nvme<ctrl>n<ns>  e.g. nvme0n1, nvme1n2
	// NVMe partitions:  nvme<ctrl>n<ns>p<part>  e.g. nvme0n1p1
	if strings.HasPrefix(dev, "nvme") {
		// A partition has a 'p' followed by digits at the end.
		// Find the last 'p' and check if everything after it is digits.
		if idx := strings.LastIndex(dev, "p"); idx > 0 {
			suffix := dev[idx+1:]
			if len(suffix) > 0 && isAllDigits(suffix) {
				return true
			}
		}
		return false // nvme whole disk
	}

	// MD RAID and device-mapper are always whole devices.
	if strings.HasPrefix(dev, "md") || strings.HasPrefix(dev, "dm-") {
		return false
	}

	// For everything else (sda, vda, xvda, hda, …):
	// A partition ends in one or more digits. A whole disk does not.
	// e.g. sda → false, sda1 → true, sdb12 → true
	if len(dev) == 0 {
		return false
	}
	return dev[len(dev)-1] >= '0' && dev[len(dev)-1] <= '9'
}

func isAllDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func fallbackMemMB() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.Sys) / (1024 * 1024)
}

// ------------------------------------------------------------------ #
// Network interface stats — /proc/net/dev
// ------------------------------------------------------------------ #

// NetIfaceStats holds per-interface throughput and packet rates.
type NetIfaceStats struct {
	RxMBps float64 `json:"rx_mbps"`
	TxMBps float64 `json:"tx_mbps"`
	RxPkts float64 `json:"rx_pkts_s"`
	TxPkts float64 `json:"tx_pkts_s"`
}

type netEntry struct {
	rxBytes, txBytes     uint64
	rxPackets, txPackets uint64
}

var prevNet struct {
	mu    sync.Mutex
	stats map[string]netEntry
	at    time.Time
}

// SampleNetStats returns per-interface throughput (MB/s) and packet rates
// since the last call. Loopback and zero-traffic interfaces are excluded.
// First call always returns nil (establishes baseline).
func SampleNetStats() map[string]NetIfaceStats {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return nil
	}
	defer f.Close()

	cur := make(map[string]netEntry)
	sc := bufio.NewScanner(f)
	// Skip the two header lines.
	sc.Scan()
	sc.Scan()
	for sc.Scan() {
		line := sc.Text()
		colonIdx := strings.Index(line, ":")
		if colonIdx < 0 {
			continue
		}
		iface := strings.TrimSpace(line[:colonIdx])
		if iface == "lo" {
			continue // skip loopback
		}
		fields := strings.Fields(line[colonIdx+1:])
		if len(fields) < 16 {
			continue
		}
		rxB, _ := strconv.ParseUint(fields[0], 10, 64)
		rxP, _ := strconv.ParseUint(fields[1], 10, 64)
		txB, _ := strconv.ParseUint(fields[8], 10, 64)
		txP, _ := strconv.ParseUint(fields[9], 10, 64)
		cur[iface] = netEntry{rxBytes: rxB, txBytes: txB, rxPackets: rxP, txPackets: txP}
	}

	now := time.Now()
	prevNet.mu.Lock()
	prev := prevNet.stats
	prevAt := prevNet.at
	prevNet.stats = cur
	prevNet.at = now
	prevNet.mu.Unlock()

	if prev == nil || prevAt.IsZero() {
		return nil
	}
	elapsed := now.Sub(prevAt).Seconds()
	if elapsed <= 0 {
		return nil
	}

	result := make(map[string]NetIfaceStats)
	for iface, c := range cur {
		p, ok := prev[iface]
		if !ok {
			continue
		}
		rxMBps := float64(c.rxBytes-p.rxBytes) / (1e6 * elapsed)
		txMBps := float64(c.txBytes-p.txBytes) / (1e6 * elapsed)
		rxPkts := float64(c.rxPackets-p.rxPackets) / elapsed
		txPkts := float64(c.txPackets-p.txPackets) / elapsed
		// Only include interfaces with actual traffic.
		if rxMBps > 0 || txMBps > 0 {
			result[iface] = NetIfaceStats{
				RxMBps: rxMBps,
				TxMBps: txMBps,
				RxPkts: rxPkts,
				TxPkts: txPkts,
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

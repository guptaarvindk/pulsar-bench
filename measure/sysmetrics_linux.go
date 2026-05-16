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

// SampleCPUPct returns the CPU usage percentage since last call (all cores combined, 0–100*ncpu).
// First call always returns 0.
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
	return (1 - deltaIdle/deltaTotal) * 100.0 * float64(runtime.NumCPU())
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

// isPartition returns true for sda1, nvme0n1p1, etc.
func isPartition(dev string) bool {
	for i := len(dev) - 1; i >= 0; i-- {
		c := dev[i]
		if c >= '0' && c <= '9' {
			continue
		}
		// nvme devices end in pN for partitions (nvme0n1p1)
		if c == 'p' && i > 0 {
			return true
		}
		// sda1, sdb2 — last char was digit, prev char is letter
		if i < len(dev)-1 {
			return true
		}
		return false
	}
	return false
}

func fallbackMemMB() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.Sys) / (1024 * 1024)
}

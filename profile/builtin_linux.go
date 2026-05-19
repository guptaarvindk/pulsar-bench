//go:build linux

package profile

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// availableRAMBytes reads MemAvailable from /proc/meminfo.
// Falls back to 32 GB if unreadable.
func availableRAMBytes() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 32 << 30
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 32 << 30
}

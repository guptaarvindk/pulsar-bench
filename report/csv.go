package report

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/minio/pulsar/measure"
)

// WriteCSV writes the per-second MetricSample time series to a CSV file.
// Columns: t_s, read_gbps, write_gbps, read_iops, write_iops,
//
//	ttfb_p50_ms, ttfb_p99_ms, op_p50_ms, op_p99_ms,
//	cpu_pct, mem_mb
//
// Returns nil if samples is empty (no file is created).
func WriteCSV(path string, samples []measure.MetricSample) error {
	if len(samples) == 0 {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	header := []string{
		"t_s", "read_gbps", "write_gbps", "read_iops", "write_iops",
		"ttfb_p50_ms", "ttfb_p99_ms", "op_p50_ms", "op_p99_ms",
		"cpu_pct", "mem_mb",
	}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, s := range samples {
		row := []string{
			f64(s.T),
			f64(s.ReadGBps),
			f64(s.WriteGBps),
			f64(s.ReadIOPS),
			f64(s.WriteIOPS),
			f64(s.TTFBP50Ms),
			f64(s.TTFBP99Ms),
			f64(s.OpP50Ms),
			f64(s.OpP99Ms),
			f64(s.CPUPct),
			f64(s.MemMB),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func f64(v float64) string {
	return strconv.FormatFloat(v, 'f', 4, 64)
}

// WriteCSVResult is a convenience wrapper that also prints to stderr on error.
func WriteCSVResult(path string, samples []measure.MetricSample) {
	if path == "" {
		return
	}
	if err := WriteCSV(path, samples); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not write CSV: %v\n", err)
		return
	}
}

package report

import (
	"encoding/csv"
	"os"
	"strconv"
	"testing"

	"github.com/minio/pulsar/measure"
)

func TestWriteCSV_EmptySamplesNoFile(t *testing.T) {
	path := t.TempDir() + "/metrics.csv"
	err := WriteCSV(path, nil)
	if err != nil {
		t.Fatalf("WriteCSV with nil samples: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("expected no file to be created for empty samples")
	}
}

func TestWriteCSV_CreatesFileWithHeader(t *testing.T) {
	samples := []measure.MetricSample{
		{T: 1, ReadGBps: 3.5, WriteGBps: 0, ReadIOPS: 1000, WriteIOPS: 0,
			TTFBP50Ms: 1.2, TTFBP99Ms: 25.0, OpP50Ms: 1.5, OpP99Ms: 30.0,
			CPUPct: 42.0, MemMB: 1024.0},
	}
	path := t.TempDir() + "/metrics.csv"
	if err := WriteCSV(path, samples); err != nil {
		t.Fatalf("WriteCSV error: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open CSV: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("read CSV: %v", err)
	}
	if len(records) != 2 { // header + 1 row
		t.Fatalf("expected 2 rows (header + data), got %d", len(records))
	}

	header := records[0]
	wantCols := []string{"t_s", "read_gbps", "write_gbps", "read_iops", "write_iops",
		"ttfb_p50_ms", "ttfb_p99_ms", "op_p50_ms", "op_p99_ms", "cpu_pct", "mem_mb"}
	if len(header) != len(wantCols) {
		t.Fatalf("header columns = %d, want %d", len(header), len(wantCols))
	}
	for i, col := range wantCols {
		if header[i] != col {
			t.Errorf("header[%d] = %q, want %q", i, header[i], col)
		}
	}
}

func TestWriteCSV_CorrectValues(t *testing.T) {
	samples := []measure.MetricSample{
		{T: 5.0, ReadGBps: 2.5, WriteGBps: 1.0, ReadIOPS: 500, WriteIOPS: 200,
			TTFBP50Ms: 2.0, TTFBP99Ms: 50.0, OpP50Ms: 3.0, OpP99Ms: 60.0,
			CPUPct: 30.0, MemMB: 512.0},
	}
	path := t.TempDir() + "/metrics.csv"
	if err := WriteCSV(path, samples); err != nil {
		t.Fatalf("WriteCSV error: %v", err)
	}

	f, _ := os.Open(path)
	defer f.Close()
	records, _ := csv.NewReader(f).ReadAll()
	row := records[1] // data row (index 0 = header)

	checkFloat := func(colName string, idx int, want float64) {
		v, err := strconv.ParseFloat(row[idx], 64)
		if err != nil {
			t.Errorf("%s: parse error: %v", colName, err)
			return
		}
		if v < want-0.001 || v > want+0.001 {
			t.Errorf("%s = %f, want %f", colName, v, want)
		}
	}
	checkFloat("t_s", 0, 5.0)
	checkFloat("read_gbps", 1, 2.5)
	checkFloat("write_gbps", 2, 1.0)
	checkFloat("cpu_pct", 9, 30.0)
	checkFloat("mem_mb", 10, 512.0)
}

func TestWriteCSV_MultipleSamples(t *testing.T) {
	samples := make([]measure.MetricSample, 10)
	for i := range samples {
		samples[i] = measure.MetricSample{T: float64(i + 1), ReadGBps: float64(i) * 0.1}
	}
	path := t.TempDir() + "/metrics.csv"
	if err := WriteCSV(path, samples); err != nil {
		t.Fatalf("WriteCSV error: %v", err)
	}

	f, _ := os.Open(path)
	defer f.Close()
	records, _ := csv.NewReader(f).ReadAll()
	if len(records) != 11 { // header + 10 rows
		t.Errorf("rows = %d, want 11", len(records))
	}
}

func TestWriteCSV_InvalidPath(t *testing.T) {
	err := WriteCSV("/nonexistent-dir/metrics.csv", []measure.MetricSample{{T: 1}})
	if err == nil {
		t.Error("expected error for invalid path, got nil")
	}
}

func TestWriteCSVResult_EmptyPath(t *testing.T) {
	// Should be a no-op, not panic
	WriteCSVResult("", []measure.MetricSample{{T: 1}})
}

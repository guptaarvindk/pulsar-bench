package report

import (
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/pulsar/workload"
)

// SweepPoint is one result file distilled to the metrics we chart.
type SweepPoint struct {
	Label        string  `json:"label"`         // filename stem or block size string
	BlockSizeBytes int64 `json:"block_size_bytes"`
	ReadGBps     float64 `json:"read_gbps"`
	WriteGBps    float64 `json:"write_gbps"`
	TTFBP50Ms    float64 `json:"ttfb_p50_ms"`
	TTFBP99Ms    float64 `json:"ttfb_p99_ms"`
	OpP50Ms      float64 `json:"op_p50_ms"`
	OpP99Ms      float64 `json:"op_p99_ms"`
	Workers      int     `json:"workers"`
	Profile      string  `json:"profile"`
}

// LoadSweepPoint loads one result JSON and converts it to a SweepPoint.
// label overrides the default (filename stem).
func LoadSweepPoint(path, label string) (SweepPoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SweepPoint{}, fmt.Errorf("reading %s: %w", path, err)
	}
	var r workload.Result
	if err := json.Unmarshal(data, &r); err != nil {
		return SweepPoint{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if label == "" {
		stem := filepath.Base(path)
		stem = strings.TrimSuffix(stem, filepath.Ext(stem))
		// If block_size_bytes is set, use a human-readable label
		if r.BlockSizeBytes > 0 {
			label = humanSweepSize(r.BlockSizeBytes)
		} else {
			label = stem
		}
	}
	return SweepPoint{
		Label:          label,
		BlockSizeBytes: r.BlockSizeBytes,
		ReadGBps:       r.Throughput.ReadGBps,
		WriteGBps:      r.Throughput.WriteGBps,
		TTFBP50Ms:      r.TTFB.P50Ms,
		TTFBP99Ms:      r.TTFB.P99Ms,
		OpP50Ms:        r.OpLatency.P50Ms,
		OpP99Ms:        r.OpLatency.P99Ms,
		Workers:        r.Workers,
		Profile:        r.Profile,
	}, nil
}

func humanSweepSize(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%d GiB", b>>30)
	case b >= 1<<20:
		return fmt.Sprintf("%d MiB", b>>20)
	case b >= 1<<10:
		return fmt.Sprintf("%d KiB", b>>10)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// WriteSweepHTML renders a multi-result sweep chart to an HTML file.
func WriteSweepHTML(outPath, title string, points []SweepPoint) error {
	pointsJSON, err := json.Marshal(points)
	if err != nil {
		return err
	}

	tmpl := template.Must(template.New("sweep").Funcs(template.FuncMap{
		"js": func(v []byte) template.JS { return template.JS(v) },
	}).Parse(sweepHTMLTemplate))

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return tmpl.Execute(f, map[string]interface{}{
		"Title":      title,
		"PointsJSON": template.JS(pointsJSON),
	})
}

const sweepHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{.Title}}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;padding:24px}
h1{font-size:1.4rem;font-weight:600;color:#fff;margin-bottom:4px}
.subtitle{font-size:.85rem;color:#64748b;margin-bottom:24px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:28px}
.card{background:#1e2130;border:1px solid #2d3148;border-radius:10px;padding:20px}
.card h2{font-size:.85rem;font-weight:500;color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;margin-bottom:14px}
canvas{max-height:260px}
table{width:100%;border-collapse:collapse;font-size:.82rem}
th{text-align:left;padding:8px 12px;color:#64748b;font-weight:500;border-bottom:1px solid #2d3148;white-space:nowrap}
td{padding:8px 12px;border-bottom:1px solid #1a1f35;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:none}
.good{color:#4ade80}.warn{color:#facc15}.bad{color:#f87171}
.target{border-left:2px solid #f59e0b;padding-left:10px;margin:16px 0;color:#fbbf24;font-size:.8rem}
footer{margin-top:24px;font-size:.75rem;color:#334155;text-align:center}
@media(max-width:720px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<h1>{{.Title}}</h1>
<p class="subtitle">Block size sweep — TTFB &amp; throughput across I/O sizes</p>

<div class="grid">
  <div class="card">
    <h2>TTFB p99 (ms) — lower is better</h2>
    <canvas id="ttfbChart"></canvas>
  </div>
  <div class="card">
    <h2>Read Throughput (GB/s) — higher is better</h2>
    <canvas id="tpChart"></canvas>
  </div>
  <div class="card">
    <h2>Op Latency p99 (ms) — lower is better</h2>
    <canvas id="opChart"></canvas>
  </div>
  <div class="card">
    <h2>TTFB p50 vs p99 (ms)</h2>
    <canvas id="ttfbBothChart"></canvas>
  </div>
</div>

<div class="card" style="margin-bottom:20px">
  <h2>Full results table</h2>
  <table id="resultsTable">
    <thead>
      <tr>
        <th>Block Size</th>
        <th>TTFB p50 (ms)</th>
        <th>TTFB p99 (ms)</th>
        <th>Op p50 (ms)</th>
        <th>Op p99 (ms)</th>
        <th>Read (GB/s)</th>
        <th>Write (GB/s)</th>
        <th>Workers</th>
      </tr>
    </thead>
    <tbody id="tableBody"></tbody>
  </table>
</div>

<div class="target">&#9888; Customer target: TTFB p99 &lt; 2 ms</div>

<footer>Generated by <a href="https://github.com/minio/pulsar" style="color:#475569">Pulsar</a> — MinIO AIStor Benchmark</footer>

<script>
const points = {{.PointsJSON}};
const labels = points.map(p => p.label);

const COLORS = [
  '#38bdf8','#4ade80','#f59e0b','#f87171','#a78bfa',
  '#fb923c','#34d399','#e879f9','#60a5fa','#facc15'
];

function barChart(id, label, data, color, targetLine) {
  const ctx = document.getElementById(id);
  const datasets = [{label, data, backgroundColor: data.map((v,i) => COLORS[i%COLORS.length]), borderRadius:4}];
  const annotations = {};
  if (targetLine != null) {
    annotations.target = {
      type:'line', yMin:targetLine, yMax:targetLine,
      borderColor:'#f59e0b', borderWidth:2, borderDash:[6,3],
      label:{content:'2ms target', display:true, color:'#f59e0b', font:{size:11}}
    };
  }
  new Chart(ctx, {
    type:'bar',
    data:{labels, datasets},
    options:{
      responsive:true, maintainAspectRatio:true,
      plugins:{legend:{display:false}, tooltip:{callbacks:{label:ctx=>ctx.parsed.y.toFixed(3)}}},
      scales:{y:{beginAtZero:true, grid:{color:'#1e2130'}, ticks:{color:'#64748b'}}, x:{grid:{display:false}, ticks:{color:'#94a3b8'}}}
    }
  });
}

function lineChart(id, datasets) {
  const ctx = document.getElementById(id);
  new Chart(ctx, {
    type:'line',
    data:{
      labels,
      datasets: datasets.map((d,i)=>({
        label:d.label, data:d.data,
        borderColor:COLORS[i], backgroundColor:'transparent',
        tension:0.3, pointRadius:5, pointHoverRadius:7, borderWidth:2
      }))
    },
    options:{
      responsive:true, maintainAspectRatio:true,
      plugins:{legend:{labels:{color:'#94a3b8', font:{size:11}}}},
      scales:{y:{beginAtZero:true, grid:{color:'#1e2130'}, ticks:{color:'#64748b'}}, x:{grid:{display:false}, ticks:{color:'#94a3b8'}}}
    }
  });
}

barChart('ttfbChart',  'TTFB p99 (ms)',    points.map(p=>p.ttfb_p99_ms), COLORS[0], 2.0);
barChart('tpChart',    'Read GB/s',         points.map(p=>p.read_gbps),   COLORS[1], null);
barChart('opChart',    'Op latency p99',    points.map(p=>p.op_p99_ms),   COLORS[3], 2.0);
lineChart('ttfbBothChart', [
  {label:'TTFB p50', data: points.map(p=>p.ttfb_p50_ms)},
  {label:'TTFB p99', data: points.map(p=>p.ttfb_p99_ms)},
]);

// Table
const tbody = document.getElementById('tableBody');
points.forEach(p => {
  const ttfbOk = p.ttfb_p99_ms < 2.0;
  const row = document.createElement('tr');
  row.innerHTML =
    '<td><strong>' + p.label + '</strong></td>' +
    '<td>' + p.ttfb_p50_ms.toFixed(3) + '</td>' +
    '<td class="' + (ttfbOk?'good':'bad') + '">' + p.ttfb_p99_ms.toFixed(3) + ' ' + (ttfbOk?'✓':'✗') + '</td>' +
    '<td>' + p.op_p50_ms.toFixed(3) + '</td>' +
    '<td>' + p.op_p99_ms.toFixed(3) + '</td>' +
    '<td>' + p.read_gbps.toFixed(3) + '</td>' +
    '<td>' + p.write_gbps.toFixed(3) + '</td>' +
    '<td>' + p.workers + '</td>';
  tbody.appendChild(row);
});
</script>
</body>
</html>`

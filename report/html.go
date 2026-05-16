package report

import (
	"encoding/json"
	"html/template"
	"os"
	"time"

	"github.com/minio/pulsar/workload"
)

// WriteHTML generates a self-contained HTML benchmark report.
func WriteHTML(path, title string, r *workload.Result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tmpl, err := template.New("report").Parse(htmlTemplate)
	if err != nil {
		return err
	}

	samplesJSON, _ := json.Marshal(r.Samples)
	summaryJSON, _ := json.Marshal(buildSummary(r))

	data := map[string]any{
		"Title":       title,
		"GeneratedAt": time.Now().Format("2006-01-02 15:04:05 MST"),
		"Summary":     template.JS(summaryJSON),
		"Samples":     template.JS(samplesJSON),
		"HasSamples":  len(r.Samples) > 0,
	}
	return tmpl.Execute(f, data)
}

type reportSummary struct {
	Profile      string   `json:"profile"`
	WorkloadType string   `json:"workload_type"`
	Path         string   `json:"path"`
	Workers      int      `json:"workers"`
	DurationS    float64  `json:"duration_s"`
	DirectIO     bool     `json:"direct_io"`
	ReadGBps     float64  `json:"read_gbps"`
	WriteGBps    float64  `json:"write_gbps"`
	TTFBP99Ms    float64  `json:"ttfb_p99_ms"`
	GPUStallPct  float64  `json:"gpu_stall_pct"`
	Pass         bool     `json:"pass"`
	Violations   []string `json:"violations"`
	StartedAt    string   `json:"started_at"`
}

func buildSummary(r *workload.Result) reportSummary {
	return reportSummary{
		Profile:      r.Profile,
		WorkloadType: r.WorkloadType,
		Path:         r.Path,
		Workers:      r.Workers,
		DurationS:    r.DurationS,
		DirectIO:     r.DirectIO,
		ReadGBps:     r.Throughput.ReadGBps,
		WriteGBps:    r.Throughput.WriteGBps,
		TTFBP99Ms:    r.TTFB.P99Ms,
		GPUStallPct:  r.GPUStallPct,
		Pass:         r.TargetsMissed == 0,
		Violations:   r.Violations,
		StartedAt:    r.StartedAt.Format("2006-01-02 15:04:05"),
	}
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{.Title}}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg: #f8f9fa;
  --card: #ffffff;
  --border: #e2e8f0;
  --text: #1a202c;
  --muted: #718096;
  --accent: #3b82f6;
  --green: #10b981;
  --red: #ef4444;
  --yellow: #f59e0b;
  --shadow: 0 1px 3px rgba(0,0,0,0.1);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); line-height: 1.5; }
.container { max-width: 1200px; margin: 0 auto; padding: 24px; }
header { margin-bottom: 32px; border-bottom: 2px solid var(--border); padding-bottom: 20px; }
header h1 { font-size: 1.75rem; font-weight: 700; color: var(--text); }
header .meta { color: var(--muted); font-size: 0.875rem; margin-top: 4px; }
.badge { display: inline-block; padding: 3px 10px; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }
.badge-pass { background: #d1fae5; color: #065f46; }
.badge-fail { background: #fee2e2; color: #991b1b; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 32px; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: var(--shadow); }
.card .label { font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 6px; }
.card .value { font-size: 1.75rem; font-weight: 700; color: var(--text); }
.card .unit { font-size: 0.875rem; color: var(--muted); font-weight: 400; }
.violations { background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 16px 20px; margin-bottom: 24px; }
.violations h3 { color: #991b1b; font-size: 0.875rem; font-weight: 600; margin-bottom: 8px; }
.violations ul { list-style: none; }
.violations li { color: #b91c1c; font-size: 0.875rem; padding: 2px 0; }
.violations li::before { content: "✗ "; }
.charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(520px, 1fr)); gap: 24px; margin-bottom: 32px; }
.chart-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: var(--shadow); }
.chart-card h3 { font-size: 0.875rem; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 16px; }
.chart-wrap { position: relative; height: 220px; }
.no-data { color: var(--muted); font-size: 0.875rem; text-align: center; padding: 60px 0; }
footer { text-align: center; color: var(--muted); font-size: 0.8rem; padding: 24px 0; border-top: 1px solid var(--border); }
</style>
</head>
<body>
<div class="container">
<header>
  <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
    <h1>{{.Title}}</h1>
    <span id="statusBadge" class="badge">—</span>
  </div>
  <div class="meta">Generated {{.GeneratedAt}}</div>
</header>

<div class="cards">
  <div class="card"><div class="label">Read Throughput</div><div class="value" id="cardRead">—</div><div class="unit">GB/s</div></div>
  <div class="card"><div class="label">Write Throughput</div><div class="value" id="cardWrite">—</div><div class="unit">GB/s</div></div>
  <div class="card"><div class="label">TTFB p99</div><div class="value" id="cardTTFB">—</div><div class="unit">ms</div></div>
  <div class="card"><div class="label">GPU Stall</div><div class="value" id="cardStall">—</div><div class="unit">%</div></div>
  <div class="card"><div class="label">Workers</div><div class="value" id="cardWorkers">—</div></div>
  <div class="card"><div class="label">Duration</div><div class="value" id="cardDuration">—</div><div class="unit">s</div></div>
  <div class="card"><div class="label">Direct I/O</div><div class="value" id="cardDIO">—</div></div>
  <div class="card"><div class="label">Profile</div><div class="value" id="cardProfile" style="font-size:1rem;line-height:1.8">—</div></div>
</div>

<div id="violationsBox" class="violations" style="display:none">
  <h3>Target Violations</h3>
  <ul id="violationsList"></ul>
</div>

{{if .HasSamples}}
<div class="charts">
  <div class="chart-card">
    <h3>Throughput (GB/s)</h3>
    <div class="chart-wrap"><canvas id="chartThroughput"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>TTFB (ms) — p50 &amp; p99</h3>
    <div class="chart-wrap"><canvas id="chartTTFB"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>IOPS</h3>
    <div class="chart-wrap"><canvas id="chartIOPS"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>Op Latency (ms) — p50 &amp; p99</h3>
    <div class="chart-wrap"><canvas id="chartOpLat"></canvas></div>
  </div>
  <div class="chart-card" id="cpuCard">
    <h3>CPU Usage (%)</h3>
    <div class="chart-wrap"><canvas id="chartCPU"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>Memory Usage (MB)</h3>
    <div class="chart-wrap"><canvas id="chartMem"></canvas></div>
  </div>
  <div class="chart-card" id="diskCard">
    <h3>Disk IOPS per Drive</h3>
    <div class="chart-wrap"><canvas id="chartDisk"></canvas></div>
  </div>
  <div class="chart-card" id="netCard">
    <h3>Network Throughput (MB/s) — RX &amp; TX per Interface</h3>
    <div class="chart-wrap"><canvas id="chartNet"></canvas></div>
  </div>
</div>
{{else}}
<div class="card"><p class="no-data">No time-series samples in this result. Re-run the benchmark to collect per-second data.</p></div>
{{end}}

<footer>Pulsar AI Storage Benchmark &mdash; <a href="https://github.com/guptaarvindk/pulsar-bench" style="color:inherit">github.com/guptaarvindk/pulsar-bench</a></footer>
</div>

<script>
const summary = {{.Summary}};
const samples = {{.Samples}};

// Populate summary cards
document.getElementById('cardRead').textContent    = summary.read_gbps > 0 ? summary.read_gbps.toFixed(2) : '—';
document.getElementById('cardWrite').textContent   = summary.write_gbps > 0 ? summary.write_gbps.toFixed(2) : '—';
document.getElementById('cardTTFB').textContent    = summary.ttfb_p99_ms > 0 ? summary.ttfb_p99_ms.toFixed(1) : '—';
document.getElementById('cardStall').textContent   = summary.gpu_stall_pct > 0 ? summary.gpu_stall_pct.toFixed(1) : '—';
document.getElementById('cardWorkers').textContent = summary.workers;
document.getElementById('cardDuration').textContent= summary.duration_s.toFixed(0);
document.getElementById('cardDIO').textContent     = summary.direct_io ? 'Yes' : 'No';
document.getElementById('cardProfile').textContent = summary.profile;

const badge = document.getElementById('statusBadge');
badge.textContent = summary.pass ? 'PASS' : 'FAIL';
badge.className   = 'badge ' + (summary.pass ? 'badge-pass' : 'badge-fail');

if (summary.violations && summary.violations.length > 0) {
  document.getElementById('violationsBox').style.display = '';
  const ul = document.getElementById('violationsList');
  summary.violations.forEach(v => {
    const li = document.createElement('li');
    li.textContent = v;
    ul.appendChild(li);
  });
}

if (!samples || samples.length === 0) {
  // No time-series, nothing more to do
} else {
  // Common chart defaults
  Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
  Chart.defaults.font.size = 12;
  Chart.defaults.color = '#718096';

  const labels = samples.map(s => s.t.toFixed(1) + 's');
  const cfg = (datasets, yLabel) => ({
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 12, padding: 16 } }, tooltip: { callbacks: { title: t => 'T+' + t[0].label } } },
      scales: {
        x: { grid: { color: '#e2e8f0' }, ticks: { maxTicksLimit: 10 } },
        y: { grid: { color: '#e2e8f0' }, title: { display: !!yLabel, text: yLabel } }
      }
    }
  });

  const ds = (label, data, color, dashed) => ({
    label, data,
    borderColor: color,
    backgroundColor: color + '18',
    borderWidth: 2,
    pointRadius: 0,
    tension: 0.3,
    fill: false,
    borderDash: dashed ? [4,3] : undefined
  });

  // 1. Throughput
  new Chart(document.getElementById('chartThroughput'), cfg([
    ds('Read GB/s',  samples.map(s => s.read_gbps),  '#3b82f6'),
    ds('Write GB/s', samples.map(s => s.write_gbps), '#f59e0b', true),
  ], 'GB/s'));

  // 2. TTFB
  new Chart(document.getElementById('chartTTFB'), cfg([
    ds('p50 ms', samples.map(s => s.ttfb_p50_ms), '#10b981'),
    ds('p99 ms', samples.map(s => s.ttfb_p99_ms), '#ef4444', true),
  ], 'ms'));

  // 3. IOPS
  new Chart(document.getElementById('chartIOPS'), cfg([
    ds('Read IOPS',  samples.map(s => s.read_iops),  '#3b82f6'),
    ds('Write IOPS', samples.map(s => s.write_iops), '#f59e0b', true),
  ], 'ops/s'));

  // 4. Op Latency
  new Chart(document.getElementById('chartOpLat'), cfg([
    ds('p50 ms', samples.map(s => s.op_p50_ms), '#10b981'),
    ds('p99 ms', samples.map(s => s.op_p99_ms), '#ef4444', true),
  ], 'ms'));

  // 5. CPU
  const cpuData = samples.map(s => s.cpu_pct);
  if (cpuData.some(v => v > 0)) {
    new Chart(document.getElementById('chartCPU'), cfg([
      ds('CPU %', cpuData, '#8b5cf6'),
    ], '%'));
  } else {
    document.getElementById('cpuCard').innerHTML = '<h3>CPU Usage</h3><div class="no-data">Not available on this platform</div>';
  }

  // 6. Memory
  new Chart(document.getElementById('chartMem'), cfg([
    ds('Memory MB', samples.map(s => s.mem_mb), '#06b6d4'),
  ], 'MB'));

  // 7. Disk IOPS
  const diskDevices = [...new Set(samples.flatMap(s => s.disk_iops ? Object.keys(s.disk_iops) : []))];
  const diskColors = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#ec4899','#84cc16'];
  if (diskDevices.length > 0) {
    const diskDatasets = diskDevices.map((dev, i) => ds(
      dev,
      samples.map(s => (s.disk_iops && s.disk_iops[dev]) || 0),
      diskColors[i % diskColors.length]
    ));
    new Chart(document.getElementById('chartDisk'), cfg(diskDatasets, 'IOPS'));
  } else {
    document.getElementById('diskCard').innerHTML = '<h3>Disk IOPS per Drive</h3><div class="no-data">Not available (requires Linux /proc/diskstats)</div>';
  }

  // 8. Network throughput — one RX line + one TX line (dashed) per interface
  const netIfaces = [...new Set(samples.flatMap(s => s.net_ifaces ? Object.keys(s.net_ifaces) : []))];
  const netPalette = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#ec4899','#84cc16'];
  if (netIfaces.length > 0) {
    const netDatasets = netIfaces.flatMap((iface, i) => {
      const color = netPalette[i % netPalette.length];
      return [
        ds(iface + ' RX', samples.map(s => (s.net_ifaces && s.net_ifaces[iface]) ? s.net_ifaces[iface].rx_mbps : 0), color),
        ds(iface + ' TX', samples.map(s => (s.net_ifaces && s.net_ifaces[iface]) ? s.net_ifaces[iface].tx_mbps : 0), color, true),
      ];
    });
    new Chart(document.getElementById('chartNet'), cfg(netDatasets, 'MB/s'));
  } else {
    document.getElementById('netCard').innerHTML = '<h3>Network Throughput</h3><div class="no-data">Not available (requires Linux /proc/net/dev)</div>';
  }
}
</script>
</body>
</html>`

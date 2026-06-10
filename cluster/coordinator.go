package cluster

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/profile"
	"github.com/minio/pulsar/report"
	"github.com/minio/pulsar/workload"
)

// httpClient is used for all coordinator→agent calls.
// The stream timeout is intentionally long (profile duration + 5 min headroom).
var (
	httpClient = &http.Client{Timeout: 30 * time.Second}
	// streamClient has no timeout because the stream stays open for the full run.
	streamClient = &http.Client{Timeout: 0}
)

// NodeAddr is host:port for an agent node.
type NodeAddr string

// Coordinator drives a multi-node benchmark run.
type Coordinator struct {
	Nodes   []NodeAddr
	Profile *profile.Profile
	Paths   []string
	Quiet   bool
}

func (c *Coordinator) Run() (*workload.Result, error) {
	n := len(c.Nodes)

	// resetAll sends /api/reset to every node — best-effort cleanup on error.
	resetAll := func() {
		for _, node := range c.Nodes {
			sendReset(string(node)) //nolint:errcheck
		}
	}

	// 1. Clock skew check — all nodes must be within 2s of coordinator
	for _, node := range c.Nodes {
		skew, err := checkClockSkew(string(node))
		if err != nil {
			return nil, fmt.Errorf("node %s unreachable: %w", node, err)
		}
		if skew > 2*time.Second {
			return nil, fmt.Errorf("node %s clock skew %v exceeds 2s — sync NTP", node, skew)
		}
	}

	// 2. Send config to all nodes
	for _, node := range c.Nodes {
		cfg := AgentConfig{
			Profile: *c.Profile,
			Paths:   c.Paths,
		}
		if err := sendConfig(string(node), cfg); err != nil {
			resetAll()
			return nil, fmt.Errorf("node %s config failed: %w", node, err)
		}
	}

	// 3. Send start time (now + 3s) to all nodes simultaneously
	startAt := time.Now().Add(3 * time.Second)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i, node := range c.Nodes {
		wg.Add(1)
		go func(i int, node NodeAddr) {
			defer wg.Done()
			errs[i] = sendStart(string(node), startAt)
		}(i, node)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			resetAll()
			return nil, fmt.Errorf("node %s start failed: %w", c.Nodes[i], err)
		}
	}

	// 4. Collect results from all nodes (streaming NDJSON), rendering a live
	//    cluster-aggregate line as per-second samples arrive from each node.
	var lp *report.LivePrinter
	if !c.Quiet {
		lp = report.NewLivePrinter(c.Profile.Duration)
		lp.Start()
	}
	latest := make([]measure.MetricSample, n)
	var amu sync.Mutex
	outcomes := make([]nodeOutcome, n)
	for i, node := range c.Nodes {
		wg.Add(1)
		go func(i int, node NodeAddr) {
			defer wg.Done()
			result, err := streamNode(string(node), func(s measure.MetricSample) {
				if lp == nil {
					return
				}
				amu.Lock()
				latest[i] = s
				merged := mergeLiveSamples(latest)
				amu.Unlock()
				lp.Update(merged)
			})
			outcomes[i] = nodeOutcome{result, err}
		}(i, node)
	}
	wg.Wait()
	if lp != nil {
		lp.Stop()
	}

	// 5. Merge results
	return mergeResults(c.Nodes, outcomes, c.Profile)
}

func checkClockSkew(node string) (time.Duration, error) {
	t0 := time.Now()
	resp, err := httpClient.Get("http://" + node + "/api/time")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	rtt := time.Since(t0)
	var reply TimeReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return 0, err
	}
	agentTime := time.Unix(0, reply.UnixNano)
	skew := agentTime.Add(rtt / 2).Sub(time.Now())
	if skew < 0 {
		skew = -skew
	}
	return skew, nil
}

func sendConfig(node string, cfg AgentConfig) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	resp, err := httpClient.Post("http://"+node+"/api/config", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("config returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func sendStart(node string, at time.Time) error {
	body, err := json.Marshal(StartRequest{AtUnixNano: at.UnixNano()})
	if err != nil {
		return err
	}
	resp, err := httpClient.Post("http://"+node+"/api/start", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("start returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func sendReset(node string) error {
	resp, err := httpClient.Post("http://"+node+"/api/reset", "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func streamNode(node string, onLive func(measure.MetricSample)) (*workload.Result, error) {
	resp, err := streamClient.Get("http://" + node + "/api/stream")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result *workload.Result
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)
	for sc.Scan() {
		var msg StreamMsg
		if err := json.Unmarshal(sc.Bytes(), &msg); err != nil {
			continue
		}
		if msg.Error != "" {
			return nil, fmt.Errorf("node error: %s", msg.Error)
		}
		if msg.Sample != nil && onLive != nil {
			onLive(*msg.Sample)
		}
		if msg.Result != nil {
			result = msg.Result
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type nodeOutcome struct {
	result *workload.Result
	err    error
}

// mergeLiveSamples combines the latest per-node sample into one cluster
// aggregate for the live display: throughput and IOPS sum across nodes,
// latency percentiles take the worst (max), CPU is averaged, T is the max.
func mergeLiveSamples(latest []measure.MetricSample) measure.MetricSample {
	var m measure.MetricSample
	var cpuSum float64
	var cpuN int
	for _, s := range latest {
		m.ReadGBps += s.ReadGBps
		m.WriteGBps += s.WriteGBps
		m.ReadIOPS += s.ReadIOPS
		m.WriteIOPS += s.WriteIOPS
		m.MemMB += s.MemMB
		if s.TTFBP50Ms > m.TTFBP50Ms {
			m.TTFBP50Ms = s.TTFBP50Ms
		}
		if s.TTFBP99Ms > m.TTFBP99Ms {
			m.TTFBP99Ms = s.TTFBP99Ms
		}
		if s.OpP50Ms > m.OpP50Ms {
			m.OpP50Ms = s.OpP50Ms
		}
		if s.OpP99Ms > m.OpP99Ms {
			m.OpP99Ms = s.OpP99Ms
		}
		if s.T > m.T {
			m.T = s.T
		}
		if s.CPUPct > 0 {
			cpuSum += s.CPUPct
			cpuN++
		}
	}
	if cpuN > 0 {
		m.CPUPct = cpuSum / float64(cpuN)
	}
	return m
}

func mergeResults(nodes []NodeAddr, outcomes []nodeOutcome, p *profile.Profile) (*workload.Result, error) {
	// Check for any errors
	for i, o := range outcomes {
		if o.err != nil {
			return nil, fmt.Errorf("node %s failed: %w", nodes[i], o.err)
		}
		if o.result == nil {
			return nil, fmt.Errorf("node %s returned no result", nodes[i])
		}
	}

	merged := &workload.Result{
		Profile:      p.Name,
		WorkloadType: p.Workload,
		Path:         outcomes[0].result.Path,
		DirectIO:     p.DirectIO,
		StartedAt:    outcomes[0].result.StartedAt,
		FinishedAt:   outcomes[0].result.FinishedAt,
		Targets:      p.Targets,
	}

	// Sum throughput across all nodes
	var totalBytesRead, totalBytesWritten, totalReadOps, totalWriteOps int64
	var totalWorkers int
	var allLatStats []measure.LatencyStats
	var allOpStats []measure.LatencyStats
	var allSamples []measure.MetricSample
	var totalGPUStall float64
	var maxDuration float64

	perNode := make([]workload.NodeResult, len(nodes))

	for i, o := range outcomes {
		r := o.result
		totalBytesRead += r.Throughput.BytesRead
		totalBytesWritten += r.Throughput.BytesWritten
		totalReadOps += r.Throughput.ReadOps
		totalWriteOps += r.Throughput.WriteOps
		totalWorkers += r.Workers
		totalGPUStall += r.GPUStallPct
		if r.DurationS > maxDuration {
			maxDuration = r.DurationS
		}
		allLatStats = append(allLatStats, r.TTFB)
		allOpStats = append(allOpStats, r.OpLatency)
		allSamples = append(allSamples, r.Samples...)

		perNode[i] = workload.NodeResult{
			Node:       string(nodes[i]),
			Throughput: r.Throughput,
			TTFB:       r.TTFB,
			OpLatency:  r.OpLatency,
		}

		// Use earliest start / latest finish
		if r.StartedAt.Before(merged.StartedAt) {
			merged.StartedAt = r.StartedAt
		}
		if r.FinishedAt.After(merged.FinishedAt) {
			merged.FinishedAt = r.FinishedAt
		}
	}

	secs := p.Duration.Seconds()
	if secs <= 0 {
		secs = maxDuration
	}

	merged.Throughput = measure.ThroughputStats{
		ElapsedS:     secs,
		BytesRead:    totalBytesRead,
		BytesWritten: totalBytesWritten,
		ReadGBps:     float64(totalBytesRead) / (1e9 * secs),
		WriteGBps:    float64(totalBytesWritten) / (1e9 * secs),
		ReadMBps:     float64(totalBytesRead) / (1e6 * secs),
		WriteMBps:    float64(totalBytesWritten) / (1e6 * secs),
		ReadOps:      totalReadOps,
		WriteOps:     totalWriteOps,
		ReadIOPS:     float64(totalReadOps) / secs,
		WriteIOPS:    float64(totalWriteOps) / secs,
	}

	merged.TTFB = mergeLatencyStats(allLatStats)
	merged.OpLatency = mergeLatencyStats(allOpStats)
	merged.Workers = totalWorkers
	merged.DurationS = maxDuration
	merged.GPUStallPct = totalGPUStall / float64(len(outcomes))
	merged.PerNode = perNode
	merged.Samples = allSamples

	// Accelerator stats
	if p.NumAccelerators > 0 && p.SampleSizeBytes > 0 && merged.DurationS > 0 {
		merged.Accelerator = &workload.AcceleratorStats{
			NumAccelerators: p.NumAccelerators,
			SamplesPerSec:   float64(merged.Throughput.BytesRead) / merged.DurationS / float64(p.SampleSizeBytes),
		}
	}

	// Violations
	merged.Violations, merged.TargetsMissed = checkTargets(merged, p)

	return merged, nil
}

// mergeLatencyStats delegates to measure.MergeLatencyStats (single source of truth).
func mergeLatencyStats(all []measure.LatencyStats) measure.LatencyStats {
	return measure.MergeLatencyStats(all)
}

func checkTargets(res *workload.Result, p *profile.Profile) ([]string, int) {
	t := p.Targets
	var v []string
	check := func(cond bool, msg string) {
		if cond {
			v = append(v, msg)
		}
	}
	check(t.ReadGBps > 0 && res.Throughput.ReadGBps < t.ReadGBps,
		fmt.Sprintf("read throughput %.2f GB/s < target %.2f GB/s", res.Throughput.ReadGBps, t.ReadGBps))
	check(t.WriteGBps > 0 && res.Throughput.WriteGBps < t.WriteGBps,
		fmt.Sprintf("write throughput %.2f GB/s < target %.2f GB/s", res.Throughput.WriteGBps, t.WriteGBps))
	check(t.TTFBColdP99Ms > 0 && res.TTFB.P99Ms > t.TTFBColdP99Ms,
		fmt.Sprintf("TTFB cold p99 %.1fms > target %.0fms", res.TTFB.P99Ms, t.TTFBColdP99Ms))
	check(t.TTFBWarmP99Ms > 0 && res.TTFB.P99Ms > t.TTFBWarmP99Ms && res.Epochs != nil,
		fmt.Sprintf("TTFB warm p99 %.1fms > target %.0fms", res.TTFB.P99Ms, t.TTFBWarmP99Ms))
	if res.Metadata != nil {
		check(t.StatP99Ms > 0 && res.Metadata.StatP99Ms > t.StatP99Ms,
			fmt.Sprintf("stat p99 %.1fms > target %.0fms", res.Metadata.StatP99Ms, t.StatP99Ms))
		check(t.ReaddirP99Ms > 0 && res.Metadata.ReaddirP99Ms > t.ReaddirP99Ms,
			fmt.Sprintf("readdir p99 %.1fms > target %.0fms", res.Metadata.ReaddirP99Ms, t.ReaddirP99Ms))
		check(t.MetaHitRatePct > 0 && res.Metadata.HitRatePct < t.MetaHitRatePct,
			fmt.Sprintf("metadata hit rate %.1f%% < target %.0f%%", res.Metadata.HitRatePct, t.MetaHitRatePct))
	}
	return v, len(v)
}

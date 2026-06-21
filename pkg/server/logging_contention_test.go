package server

import (
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/client"
	"github.com/colonyos/colonies/pkg/constants"
	"github.com/colonyos/colonies/pkg/rpc"
	"github.com/colonyos/colonies/pkg/utils"
	"github.com/stretchr/testify/assert"
)

const contentionLogLine = "2026-06-20T10:00:00Z INFO processing chunk 4213 of 9000, throughput=123MB/s"

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p / 100 * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// TestLoggingContention measures whether intensive logging by some executors
// degrades the latency of an unrelated request made by another executor, and
// whether batched logging relieves that. A "victim" executor repeatedly issues a
// read (GetProcess) while N "flooder" executors hammer the log endpoint; we
// report the victim's p50/p99 latency under three conditions: no logging,
// per-line logging, and batched logging.
//
// It is a load test (latency percentiles), not a go benchmark, and is gated:
//
//	COLONIES_CONTENTION_TEST=1 TZ=Europe/Stockholm \
//	  go test -run TestLoggingContention -timeout 300s ./pkg/server/ -v
func TestLoggingContention(t *testing.T) {
	if os.Getenv("COLONIES_CONTENTION_TEST") == "" {
		t.Skip("set COLONIES_CONTENTION_TEST=1 to run the logging contention load test")
	}

	duration := 3 * time.Second
	if v := os.Getenv("COLONIES_CONTENTION_SECONDS"); v != "" {
		if s, err := strconv.Atoi(v); err == nil {
			duration = time.Duration(s) * time.Second
		}
	}
	numFlooders := 8
	batchSize := 100

	env, setupClient, server, _, _ := setupTestEnv1(t)
	defer server.Shutdown()

	// A long-running process assigned to executor1 for the flooders to log to.
	funcSpec := utils.CreateTestFunctionSpec(env.colony1Name)
	funcSpec.MaxExecTime = -1
	_, err := setupClient.Submit(funcSpec, env.executor1PrvKey)
	assert.Nil(t, err)
	assigned, err := setupClient.Assign(env.colony1Name, -1, "", "", env.executor1PrvKey)
	assert.Nil(t, err)
	assert.NotNil(t, assigned)
	processID := assigned.ID

	// A second approved executor in colony1 acts as the victim (a colony member,
	// distinct from the flooder identity).
	victimExecutor, victimPrvKey, err := utils.CreateTestExecutorWithKey(env.colony1Name)
	assert.Nil(t, err)
	_, err = setupClient.AddExecutor(victimExecutor, env.colony1PrvKey)
	assert.Nil(t, err)
	err = setupClient.ApproveExecutor(env.colony1Name, victimExecutor.Name, env.colony1PrvKey)
	assert.Nil(t, err)

	newClient := func() *client.ColoniesClient {
		return client.CreateColoniesClient(constants.TESTHOST, constants.TESTPORT, Insecure, SkipTLSVerify)
	}

	// run executes one condition and returns the victim latency samples plus the
	// number of log lines the flooders pushed. maxLinesPerSec > 0 throttles the
	// flooders to that aggregate line rate (0 = flat out).
	run := func(mode string, maxLinesPerSec float64) ([]time.Duration, int64) {
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var totalLines int64

		flood := mode == "perline" || mode == "batched" || mode == "batched-matched"
		perFlooderRate := maxLinesPerSec / float64(numFlooders)

		if flood {
			entries := make([]rpc.LogEntry, batchSize)
			for j := range entries {
				entries[j] = rpc.LogEntry{Timestamp: int64(j + 1), Message: contentionLogLine}
			}
			for f := 0; f < numFlooders; f++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					fc := newClient()
					for {
						select {
						case <-stop:
							return
						default:
						}
						sent := 0
						if mode == "perline" {
							if err := fc.AddLog(processID, contentionLogLine, env.executor1PrvKey); err == nil {
								sent = 1
							}
						} else { // batched or batched-matched
							if err := fc.AddLogs(processID, entries, env.executor1PrvKey); err == nil {
								sent = batchSize
							}
						}
						atomic.AddInt64(&totalLines, int64(sent))
						if maxLinesPerSec > 0 && sent > 0 {
							time.Sleep(time.Duration(float64(sent) / perFlooderRate * float64(time.Second)))
						}
					}
				}()
			}
		}

		// Victim: one client issuing sequential reads, timing each.
		victimClient := newClient()
		var samples []time.Duration
		deadline := time.Now().Add(duration)
		for time.Now().Before(deadline) {
			t0 := time.Now()
			_, err := victimClient.GetProcess(processID, victimPrvKey)
			d := time.Since(t0)
			if err == nil {
				samples = append(samples, d)
			}
		}

		close(stop)
		wg.Wait()
		return samples, atomic.LoadInt64(&totalLines)
	}

	report := func(mode string, samples []time.Duration, lines int64) {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		t.Logf("%-9s victim: ops=%-5d  p50=%-10v p99=%-10v | flooder lines=%-9d (%.0f lines/s)",
			mode, len(samples), percentile(samples, 50), percentile(samples, 99),
			lines, float64(lines)/duration.Seconds())
	}

	baseSamples, _ := run("baseline", 0)
	report("baseline", baseSamples, 0)

	perlineSamples, perlineLines := run("perline", 0)
	report("perline", perlineSamples, perlineLines)

	batchedSamples, batchedLines := run("batched", 0)
	report("batched", batchedSamples, batchedLines)

	// Rate-matched: batched throttled to the same line rate per-line achieved.
	// Same logging workload, far fewer requests -> the victim should return
	// close to baseline, isolating the per-request contention that batching removes.
	matchRate := float64(perlineLines) / duration.Seconds()
	matchedSamples, matchedLines := run("batched-matched", matchRate)
	report("batched-matched", matchedSamples, matchedLines)
}

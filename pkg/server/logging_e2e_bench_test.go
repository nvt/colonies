package server

import (
	"fmt"
	"os"
	"testing"

	"github.com/colonyos/colonies/pkg/client"
	"github.com/colonyos/colonies/pkg/rpc"
	"github.com/colonyos/colonies/pkg/utils"
)

const benchE2ELogLine = "2026-06-20T10:00:00Z INFO processing chunk 4213 of 9000, throughput=123MB/s"

// benchAssignedProcess starts a server and returns a client plus a long-running
// assigned process to log against (MaxExecTime=-1 so the timeout worker never
// unassigns it mid-benchmark). The caller must defer server.Shutdown().
func benchAssignedProcess(b *testing.B) (*client.ColoniesClient, *Server, *testEnv1, string) {
	env, c, server, _, _ := setupTestEnv1(b)
	funcSpec := utils.CreateTestFunctionSpec(env.colony1Name)
	funcSpec.MaxExecTime = -1
	if _, err := c.Submit(funcSpec, env.executor1PrvKey); err != nil {
		b.Fatal(err)
	}
	assigned, err := c.Assign(env.colony1Name, -1, "", "", env.executor1PrvKey)
	if err != nil {
		b.Fatal(err)
	}
	if assigned == nil {
		b.Fatal("no process assigned")
	}
	return c, server, env, assigned.ID
}

// BenchmarkLogIngest compares old (per-line) and new (batched) log ingestion. Each
// iteration delivers a fixed block of log lines for one process over the full real
// path (signed RPC -> HTTP -> ECDSA -> authorization -> DB, including the read
// reduction and COPY for large blocks), either:
//
//	old: one signed AddLog RPC per line (the original design)
//	new: one batched AddLogs RPC for the whole block (current design)
//
// Select the path with COLONIES_BENCH_MODE=old|new. Sub-benchmark names match
// across modes, so benchstat reports the speedup directly:
//
//	TZ=Europe/Stockholm COLONIES_BENCH_MODE=old go test -run=^$ -bench=BenchmarkLogIngest -benchtime=2s -count=6 ./pkg/server/ 2>/dev/null > /tmp/old.txt
//	TZ=Europe/Stockholm COLONIES_BENCH_MODE=new go test -run=^$ -bench=BenchmarkLogIngest -benchtime=2s -count=6 ./pkg/server/ 2>/dev/null > /tmp/new.txt
//	benchstat /tmp/old.txt /tmp/new.txt
//
// ns/op is the cost to deliver one block; the ns/line metric is per log line.
func BenchmarkLogIngest(b *testing.B) {
	mode := os.Getenv("COLONIES_BENCH_MODE")
	if mode == "" {
		mode = "new"
	}
	if mode != "old" && mode != "new" {
		b.Fatalf("COLONIES_BENCH_MODE must be old or new, got %q", mode)
	}

	c, server, env, processID := benchAssignedProcess(b)
	defer server.Shutdown()

	for _, block := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("block=%d", block), func(b *testing.B) {
			entries := make([]rpc.LogEntry, block)
			for j := range entries {
				entries[j] = rpc.LogEntry{Timestamp: int64(j + 1), Message: benchE2ELogLine}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "old" {
					for j := 0; j < block; j++ {
						if err := c.AddLog(processID, benchE2ELogLine, env.executor1PrvKey); err != nil {
							b.Fatal(err)
						}
					}
				} else {
					if err := c.AddLogs(processID, entries, env.executor1PrvKey); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*block), "ns/line")
		})
	}
}

// BenchmarkE2ELogging compares per-line AddLog vs batched AddLogs over the full
// path: signed RPC -> HTTP -> ECDSA recovery -> process/membership/executor
// lookups -> INSERT. One server and one long-running assigned process are set up
// once; the sub-benchmarks reuse them, so etcd starts only once.
//
// Heavy (starts an embedded-etcd server + needs Postgres on :5432). Run
// explicitly:
//
//	TZ=Europe/Stockholm go test -run=^$ -bench=BenchmarkE2ELogging -benchtime=2s \
//	  ./pkg/server/ 2>/dev/null | grep -E 'Benchmark|ns/op|ns/line'
//
// AddLog's ns/op is ns/line; AddLogs reports a ns/line metric, directly
// comparable.
func BenchmarkE2ELogging(b *testing.B) {
	env, coloniesClient, server, _, _ := setupTestEnv1(b)
	defer server.Shutdown()

	// A long-running assigned process to log against. MaxExecTime=-1 keeps the
	// timeout worker from unassigning it mid-benchmark.
	funcSpec := utils.CreateTestFunctionSpec(env.colony1Name)
	funcSpec.MaxExecTime = -1
	if _, err := coloniesClient.Submit(funcSpec, env.executor1PrvKey); err != nil {
		b.Fatal(err)
	}
	assigned, err := coloniesClient.Assign(env.colony1Name, -1, "", "", env.executor1PrvKey)
	if err != nil {
		b.Fatal(err)
	}
	if assigned == nil {
		b.Fatal("no process was assigned")
	}
	processID := assigned.ID

	b.Run("AddLog", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := coloniesClient.AddLog(processID, benchE2ELogLine, env.executor1PrvKey); err != nil {
				b.Fatal(err)
			}
		}
	})

	for _, batch := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("AddLogs/batch=%d", batch), func(b *testing.B) {
			entries := make([]rpc.LogEntry, batch)
			for j := range entries {
				entries[j] = rpc.LogEntry{Timestamp: int64(j + 1), Message: benchE2ELogLine}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := coloniesClient.AddLogs(processID, entries, env.executor1PrvKey); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/line")
		})
	}
}

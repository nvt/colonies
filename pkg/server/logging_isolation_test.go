package server

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/client"
	"github.com/colonyos/colonies/pkg/constants"
	"github.com/colonyos/colonies/pkg/database/postgresql"
	"github.com/colonyos/colonies/pkg/logservice"
	"github.com/colonyos/colonies/pkg/rpc"
	"github.com/colonyos/colonies/pkg/utils"
)

func isoEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// connectSharedTestDB opens a second connection to the same database the test
// harness uses (prefix TEST_), without dropping/initializing, so the log service
// shares the colonies server's data.
func connectSharedTestDB(t testing.TB) *postgresql.PQDatabase {
	host := isoEnvDefault("COLONIES_DB_HOST", "localhost")
	user := isoEnvDefault("COLONIES_DB_USER", "postgres")
	password := isoEnvDefault("COLONIES_DB_PASSWORD", "rFcLGNkgsNtksg6Pgtn9CumL4xXBQ7")
	db := postgresql.CreatePQDatabase(host, 5432, user, password, "postgres", "TEST_", false)
	if err := db.Connect(); err != nil {
		t.Fatalf("log service could not connect to the shared test DB: %v", err)
	}
	return db
}

func waitForHealthHTTP(port int) bool {
	for i := 0; i < 200; i++ {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
		if err == nil {
			resp.Body.Close()
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// TestLoggingIsolation quantifies cross-executor isolation: a victim executor
// issues reads against the colonies server while flooder executors hammer the log
// endpoint, and we report the victim's latency under three conditions:
//
//	baseline:   no logging
//	in-process: flood routed to the colonies server (logs share its CPU/pool)
//	separated:  flood routed to a standalone log service (shares only the DB)
//
// "separated" should restore the victim toward baseline because the log CPU lands
// in the other process; any residual gap is the cost of sharing the database.
// Setting COLONIES_LOG_MAX_CONCURRENCY makes the colonies server apply its
// concurrency limit, so the in-process row then reflects that too.
//
// It is a load test (latency percentiles), not a go benchmark, and is gated:
//
//	COLONIES_ISOLATION_TEST=1 TZ=Europe/Stockholm \
//	  go test -run TestLoggingIsolation -timeout 300s ./pkg/server/ -v
func TestLoggingIsolation(t *testing.T) {
	if os.Getenv("COLONIES_ISOLATION_TEST") == "" {
		t.Skip("set COLONIES_ISOLATION_TEST=1 to run the logging isolation load test")
	}

	duration := 3 * time.Second
	if v := os.Getenv("COLONIES_CONTENTION_SECONDS"); v != "" {
		if s, err := strconv.Atoi(v); err == nil {
			duration = time.Duration(s) * time.Second
		}
	}
	numFlooders := 8
	batchSize := 100
	logPort := constants.TESTPORT + 5

	env, mainClient, mainServer, _, _ := setupTestEnv1(t)
	defer mainServer.Shutdown()

	// A long-running process assigned to executor1 for the flooders to log to.
	funcSpec := utils.CreateTestFunctionSpec(env.colony1Name)
	funcSpec.MaxExecTime = -1
	_, err := mainClient.Submit(funcSpec, env.executor1PrvKey)
	if err != nil {
		t.Fatal(err)
	}
	assigned, err := mainClient.Assign(env.colony1Name, -1, "", "", env.executor1PrvKey)
	if err != nil || assigned == nil {
		t.Fatalf("assign failed: %v", err)
	}
	processID := assigned.ID

	// A second approved executor in colony1 acts as the victim (reads only).
	victimExecutor, victimPrvKey, err := utils.CreateTestExecutorWithKey(env.colony1Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mainClient.AddExecutor(victimExecutor, env.colony1PrvKey); err != nil {
		t.Fatal(err)
	}
	if err := mainClient.ApproveExecutor(env.colony1Name, victimExecutor.Name, env.colony1PrvKey); err != nil {
		t.Fatal(err)
	}

	// A standalone log service sharing the same database. It honors the same QoS
	// env as the colonies server, so the "separated" row reflects QoS when set.
	logDB := connectSharedTestDB(t)
	defer logDB.Close()
	ls := logservice.NewLogService(logDB, logPort, logservice.Config{MaxConcurrency: envInt("COLONIES_LOG_MAX_CONCURRENCY", 0)})
	go ls.ServeForever()
	defer ls.Shutdown()
	if !waitForHealthHTTP(logPort) {
		t.Fatal("log service did not become healthy")
	}

	newMainClient := func() *client.ColoniesClient {
		return client.CreateColoniesClient(constants.TESTHOST, constants.TESTPORT, Insecure, SkipTLSVerify)
	}
	newLogServiceClient := func() *client.ColoniesClient {
		return client.CreateColoniesClient(constants.TESTHOST, logPort, Insecure, SkipTLSVerify)
	}

	report := func(name string, floodFactory func() *client.ColoniesClient) {
		ops, p50, p99, lines := floodVictim(duration, numFlooders, batchSize, processID, env.executor1PrvKey, victimPrvKey, floodFactory, newMainClient)
		t.Logf("%-11s victim: ops=%-5d p50=%-12v p99=%-12v | flood=%-9d (%.0f lines/s)",
			name, ops, p50, p99, lines, float64(lines)/duration.Seconds())
	}

	report("baseline", nil)
	report("in-process", newMainClient)      // flood -> colonies server
	report("separated", newLogServiceClient) // flood -> standalone log service
}

// floodVictim runs numFlooders concurrent AddLogs floods (floodFactory, nil =
// no flood) for duration, while measuring the latency of a victim that issues
// sequential GetProcess reads (victimFactory). Returns the victim op count and
// p50/p99, plus total log lines pushed.
func floodVictim(duration time.Duration, numFlooders, batchSize int, processID, floodKey, victimKey string,
	floodFactory, victimFactory func() *client.ColoniesClient) (ops int, p50, p99 time.Duration, lines int64) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var totalLines int64

	if floodFactory != nil {
		entries := make([]rpc.LogEntry, batchSize)
		for j := range entries {
			entries[j] = rpc.LogEntry{Timestamp: int64(j + 1), Message: contentionLogLine}
		}
		for f := 0; f < numFlooders; f++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				fc := floodFactory()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if err := fc.AddLogs(processID, entries, floodKey); err == nil {
						atomic.AddInt64(&totalLines, int64(batchSize))
					}
				}
			}()
		}
	}

	victim := victimFactory()
	var samples []time.Duration
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		t0 := time.Now()
		_, err := victim.GetProcess(processID, victimKey)
		if err == nil {
			samples = append(samples, time.Since(t0))
		}
	}
	close(stop)
	wg.Wait()

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return len(samples), percentile(samples, 50), percentile(samples, 99), atomic.LoadInt64(&totalLines)
}

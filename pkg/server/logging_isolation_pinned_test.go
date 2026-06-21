package server

import (
	"bytes"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/client"
	"github.com/colonyos/colonies/pkg/constants"
	"github.com/colonyos/colonies/pkg/utils"
)

// TestLoggingIsolationPinned approximates "the log service on dedicated hardware"
// on a single machine: it runs the log service as a SEPARATE OS PROCESS with a
// bounded GOMAXPROCS, and pins the colonies server (this test process) to its own
// GOMAXPROCS. On a machine with more physical cores than the two budgets combined,
// the two processes run on (largely) different cores, so a flood routed to the log
// service does not steal CPU from the colonies server.
//
// Compare the victim's p99 with the flood routed in-process vs separated: unlike
// the same-process TestLoggingIsolation, separation should now help, because the
// flood's server-side work (ECDSA recovery + handler) leaves the colonies server's
// CPU budget.
//
//	COLONIES_ISOLATION_PINNED_TEST=1 TZ=Europe/Stockholm \
//	  go test -run TestLoggingIsolationPinned -timeout 300s ./pkg/server/ -v
//
// Tunables: COLONIES_ISOLATION_SERVER_PROCS, COLONIES_ISOLATION_LOG_PROCS (default
// 2 each), COLONIES_CONTENTION_SECONDS.
func TestLoggingIsolationPinned(t *testing.T) {
	if os.Getenv("COLONIES_ISOLATION_PINNED_TEST") == "" {
		t.Skip("set COLONIES_ISOLATION_PINNED_TEST=1 to run the pinned isolation load test")
	}

	serverProcs := envInt("COLONIES_ISOLATION_SERVER_PROCS", 2)
	logProcs := envInt("COLONIES_ISOLATION_LOG_PROCS", 2)
	duration := 3 * time.Second
	if v := os.Getenv("COLONIES_CONTENTION_SECONDS"); v != "" {
		if s, err := strconv.Atoi(v); err == nil {
			duration = time.Duration(s) * time.Second
		}
	}
	numFlooders := 8
	batchSize := 100
	logPort := constants.TESTPORT + 6

	if runtime.NumCPU() < serverProcs+logProcs+1 {
		t.Skipf("need > %d CPUs to emulate dedicated hardware, have %d", serverProcs+logProcs, runtime.NumCPU())
	}

	env, mainClient, mainServer, _, _ := setupTestEnv1(t)
	defer mainServer.Shutdown()

	funcSpec := utils.CreateTestFunctionSpec(env.colony1Name)
	funcSpec.MaxExecTime = -1
	if _, err := mainClient.Submit(funcSpec, env.executor1PrvKey); err != nil {
		t.Fatal(err)
	}
	assigned, err := mainClient.Assign(env.colony1Name, -1, "", "", env.executor1PrvKey)
	if err != nil || assigned == nil {
		t.Fatalf("assign failed: %v", err)
	}
	processID := assigned.ID

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

	// Build and launch the log service as a separate OS process with its own
	// GOMAXPROCS, connected to the same (TEST_) database.
	bin := t.TempDir() + "/colonies-logservice"
	build := exec.Command("go", "build", "-o", bin, "github.com/colonyos/colonies/cmd/logservice")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("failed to build log service: %v", err)
	}

	var childErr bytes.Buffer
	child := exec.Command(bin)
	child.Stderr = &childErr
	child.Env = append(os.Environ(),
		"GOMAXPROCS="+strconv.Itoa(logProcs),
		"COLONIES_LOGSERVICE_PORT="+strconv.Itoa(logPort),
		"COLONIES_DB_PREFIX=TEST_",
		"COLONIES_DB_NAME=postgres",
		"COLONIES_DB_HOST="+isoEnvDefault("COLONIES_DB_HOST", "localhost"),
		"COLONIES_DB_PORT=5432",
		"COLONIES_DB_USER="+isoEnvDefault("COLONIES_DB_USER", "postgres"),
		"COLONIES_DB_PASSWORD="+isoEnvDefault("COLONIES_DB_PASSWORD", "rFcLGNkgsNtksg6Pgtn9CumL4xXBQ7"),
	)
	if err := child.Start(); err != nil {
		t.Fatalf("failed to start log service: %v", err)
	}
	defer func() {
		child.Process.Kill()
		child.Wait()
	}()
	if !waitForHealthHTTP(logPort) {
		t.Fatalf("log service did not become healthy; stderr:\n%s", childErr.String())
	}

	newMainClient := func() *client.ColoniesClient {
		return client.CreateColoniesClient(constants.TESTHOST, constants.TESTPORT, Insecure, SkipTLSVerify)
	}
	newLogServiceClient := func() *client.ColoniesClient {
		return client.CreateColoniesClient(constants.TESTHOST, logPort, Insecure, SkipTLSVerify)
	}

	// Pin the colonies server (this process) to its CPU budget.
	prev := runtime.GOMAXPROCS(serverProcs)
	defer runtime.GOMAXPROCS(prev)

	t.Logf("pinned: colonies server GOMAXPROCS=%d, log service GOMAXPROCS=%d (separate process), NumCPU=%d",
		serverProcs, logProcs, runtime.NumCPU())

	for _, cfg := range []struct {
		name string
		ff   func() *client.ColoniesClient
	}{
		{"baseline", nil},
		{"in-process", newMainClient},
		{"separated", newLogServiceClient},
	} {
		ops, p50, p99, lines := floodVictim(duration, numFlooders, batchSize, processID, env.executor1PrvKey, victimPrvKey, cfg.ff, newMainClient)
		t.Logf("%-11s victim: ops=%-5d p50=%-12v p99=%-12v | flood=%-9d (%.0f lines/s)",
			cfg.name, ops, p50, p99, lines, float64(lines)/duration.Seconds())
	}
}

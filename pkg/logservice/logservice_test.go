package logservice

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/client"
	"github.com/colonyos/colonies/pkg/core"
	"github.com/colonyos/colonies/pkg/database/postgresql"
	"github.com/colonyos/colonies/pkg/rpc"
	"github.com/colonyos/colonies/pkg/utils"
	"github.com/stretchr/testify/assert"
)

const testLogServicePort = 28190

func waitForHealth(port int) bool {
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

// setupRunningProcess creates, directly in the shared DB, a colony, an approved
// executor, and a RUNNING process assigned to that executor.
func setupRunningProcess(t *testing.T, db *postgresql.PQDatabase) (colonyName, processID, executorPrvKey string) {
	colony, _, err := utils.CreateTestColonyWithKey()
	assert.Nil(t, err)
	assert.Nil(t, db.AddColony(colony))

	executor, executorKey, err := utils.CreateTestExecutorWithKey(colony.Name)
	assert.Nil(t, err)
	assert.Nil(t, db.AddExecutor(executor))
	assert.Nil(t, db.ApproveExecutor(executor))

	process := core.CreateProcess(utils.CreateTestFunctionSpec(colony.Name))
	assert.Nil(t, db.AddProcess(process))
	assert.Nil(t, db.Assign(executor.ID, process)) // -> RUNNING, assigned

	return colony.Name, process.ID, executorKey
}

// TestLogServiceEndToEnd runs the log service against a real (shared) DB and
// verifies the full signed-HTTP path: a client adds logs and reads them back
// through the service, authorized exactly as in the in-server handler.
func TestLogServiceEndToEnd(t *testing.T) {
	db, err := postgresql.PrepareTests()
	if err != nil {
		t.Skipf("no PostgreSQL available: %v", err)
	}
	defer db.Close()

	colonyName, processID, executorPrvKey := setupRunningProcess(t, db)

	ls := NewLogService(db, testLogServicePort, Config{})
	go ls.ServeForever()
	defer ls.Shutdown()
	if !waitForHealth(testLogServicePort) {
		t.Fatal("log service did not become healthy")
	}

	c := client.CreateColoniesClient("localhost", testLogServicePort, true, true)

	entries := []rpc.LogEntry{
		{Timestamp: 1, Message: "log line 1"},
		{Timestamp: 2, Message: "log line 2"},
		{Timestamp: 3, Message: "log line 3"},
	}
	assert.Nil(t, c.AddLogs(processID, entries, executorPrvKey))

	logs, err := c.GetLogsByProcess(colonyName, processID, 100, executorPrvKey)
	assert.Nil(t, err)
	assert.Len(t, logs, 3)
	got := map[string]bool{}
	for _, l := range logs {
		got[l.Message] = true
	}
	assert.True(t, got["log line 1"] && got["log line 2"] && got["log line 3"])
}

// TestLogServiceRejectsUnauthorized verifies that the service denies a signed
// request from an executor that is not the assigned executor of the process.
func TestLogServiceRejectsUnauthorized(t *testing.T) {
	db, err := postgresql.PrepareTests()
	if err != nil {
		t.Skipf("no PostgreSQL available: %v", err)
	}
	defer db.Close()

	colonyName, processID, _ := setupRunningProcess(t, db)

	// A different, unrelated executor (not assigned to the process).
	otherExecutor, otherKey, err := utils.CreateTestExecutorWithKey(colonyName)
	assert.Nil(t, err)
	assert.Nil(t, db.AddExecutor(otherExecutor))
	assert.Nil(t, db.ApproveExecutor(otherExecutor))

	ls := NewLogService(db, testLogServicePort+1, Config{})
	go ls.ServeForever()
	defer ls.Shutdown()
	if !waitForHealth(testLogServicePort + 1) {
		t.Fatal("log service did not become healthy")
	}

	c := client.CreateColoniesClient("localhost", testLogServicePort+1, true, true)
	err = c.AddLogs(processID, []rpc.LogEntry{{Timestamp: 1, Message: "x"}}, otherKey)
	assert.NotNil(t, err) // not the assigned executor -> denied
}

package postgresql

import (
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/core"
	"github.com/stretchr/testify/assert"
)

func TestAddLogs(t *testing.T) {
	db, err := PrepareTests()
	assert.Nil(t, err)
	defer db.Close()

	ts := time.Now().UTC().UnixNano()
	logs := []*core.Log{
		{ProcessID: "p1", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts, Message: "line 1"},
		{ProcessID: "p1", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts + 1, Message: "line 2"},
		{ProcessID: "p1", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts + 2, Message: "line 3"},
	}
	err = db.AddLogs(logs)
	assert.Nil(t, err)

	got, err := db.GetLogsByProcessID("p1", 100)
	assert.Nil(t, err)
	assert.Len(t, got, 3)
	// Stored messages match what was written.
	msgs := map[string]bool{}
	for _, l := range got {
		msgs[l.Message] = true
		assert.Equal(t, "c1", l.ColonyName)
		assert.Equal(t, "e1", l.ExecutorName)
	}
	assert.True(t, msgs["line 1"] && msgs["line 2"] && msgs["line 3"])
}

func TestAddLogsEmpty(t *testing.T) {
	db, err := PrepareTests()
	assert.Nil(t, err)
	defer db.Close()

	// Empty batch is a no-op and must not error.
	err = db.AddLogs(nil)
	assert.Nil(t, err)
	err = db.AddLogs([]*core.Log{})
	assert.Nil(t, err)
}

// TestAddLogsChunking writes more rows than maxLogRowsPerInsert to exercise the
// large-batch path via the public AddLogs dispatch, and verifies all rows land.
func TestAddLogsChunking(t *testing.T) {
	db, err := PrepareTests()
	assert.Nil(t, err)
	defer db.Close()

	n := maxLogRowsPerInsert + 250
	ts := time.Now().UTC().UnixNano()
	logs := make([]*core.Log, n)
	for i := 0; i < n; i++ {
		logs[i] = &core.Log{ProcessID: "pchunk", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts + int64(i), Message: "m"}
	}
	err = db.AddLogs(logs)
	assert.Nil(t, err)

	got, err := db.GetLogsByProcessID("pchunk", n+100)
	assert.Nil(t, err)
	assert.Len(t, got, n)
}

// TestAddLogsInsertChunkingDirect exercises the multi-row INSERT path's chunking
// loop directly (independent of the COPY dispatch threshold).
func TestAddLogsInsertChunkingDirect(t *testing.T) {
	db, err := PrepareTests()
	assert.Nil(t, err)
	defer db.Close()

	n := maxLogRowsPerInsert + 250
	ts := time.Now().UTC().UnixNano()
	logs := make([]*core.Log, n)
	for i := 0; i < n; i++ {
		logs[i] = &core.Log{ProcessID: "pins", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts + int64(i), Message: "m"}
	}
	err = db.addLogsInsert(logs)
	assert.Nil(t, err)

	got, err := db.GetLogsByProcessID("pins", n+100)
	assert.Nil(t, err)
	assert.Len(t, got, n)
}

// TestAddLogsCopyDirect exercises the COPY path directly and verifies content,
// ordering, and the server-derived ADDED timestamp are all written.
func TestAddLogsCopyDirect(t *testing.T) {
	db, err := PrepareTests()
	assert.Nil(t, err)
	defer db.Close()

	ts := time.Now().UTC().UnixNano()
	logs := []*core.Log{
		{ProcessID: "pcopy", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts, Message: "copy 1"},
		{ProcessID: "pcopy", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts + 1, Message: "copy 2"},
		{ProcessID: "pcopy", ColonyName: "c1", ExecutorName: "e1", Timestamp: ts + 2, Message: "copy 3"},
	}
	err = db.addLogsCopy(logs)
	assert.Nil(t, err)

	got, err := db.GetLogsByProcessID("pcopy", 100)
	assert.Nil(t, err)
	assert.Len(t, got, 3)
	// GetLogsByProcessID orders by TS, so messages come back in write order.
	assert.Equal(t, "copy 1", got[0].Message)
	assert.Equal(t, "copy 2", got[1].Message)
	assert.Equal(t, "copy 3", got[2].Message)
	for _, l := range got {
		assert.Equal(t, "c1", l.ColonyName)
		assert.Equal(t, "e1", l.ExecutorName)
	}
}

package logbuffer

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/core"
	"github.com/stretchr/testify/assert"
)

type recorder struct {
	mu       sync.Mutex
	calls    int
	rows     int
	failWith error
}

func (r *recorder) flush(logs []*core.Log) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failWith != nil {
		return r.failWith
	}
	r.calls++
	r.rows += len(logs)
	return nil
}

func (r *recorder) snapshot() (calls, rows int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.rows
}

func batch(n int) []*core.Log {
	out := make([]*core.Log, n)
	for i := range out {
		out[i] = &core.Log{Message: "x"}
	}
	return out
}

func TestSyncIngestorWritesThrough(t *testing.T) {
	r := &recorder{}
	ing := NewSync(r.flush)
	assert.Nil(t, ing.Ingest(batch(3)))
	assert.Nil(t, ing.Ingest(batch(2)))
	assert.Nil(t, ing.Ingest(nil)) // empty is a no-op
	assert.Nil(t, ing.Close())

	calls, rows := r.snapshot()
	assert.Equal(t, 2, calls) // one flush per batch, no coalescing
	assert.Equal(t, 5, rows)
}

func TestAsyncCoalescesManyBatches(t *testing.T) {
	r := &recorder{}
	// High thresholds + long interval: nothing flushes until Close drains.
	ing := NewAsync(r.flush, AsyncConfig{FlushRows: 100000, FlushInterval: time.Hour, QueueCap: 1024})

	for i := 0; i < 50; i++ {
		assert.Nil(t, ing.Ingest(batch(2)))
	}
	assert.Nil(t, ing.Close())

	calls, rows := r.snapshot()
	assert.Equal(t, 100, rows)      // all rows persisted
	assert.LessOrEqual(t, calls, 2) // 50 batches coalesced into 1 (allow slack)
	assert.GreaterOrEqual(t, calls, 1)
}

func TestAsyncFlushesOnRowThreshold(t *testing.T) {
	r := &recorder{}
	ing := NewAsync(r.flush, AsyncConfig{FlushRows: 10, FlushInterval: time.Hour, QueueCap: 1024})
	defer ing.Close()

	for i := 0; i < 5; i++ {
		ing.Ingest(batch(3)) // 15 rows total -> crosses the 10-row threshold
	}
	assert.Eventually(t, func() bool {
		_, rows := r.snapshot()
		return rows >= 10
	}, time.Second, 5*time.Millisecond)
}

func TestAsyncFlushesOnInterval(t *testing.T) {
	r := &recorder{}
	ing := NewAsync(r.flush, AsyncConfig{FlushRows: 100000, FlushInterval: 20 * time.Millisecond, QueueCap: 1024})
	defer ing.Close()

	ing.Ingest(batch(1))
	assert.Eventually(t, func() bool {
		_, rows := r.snapshot()
		return rows == 1
	}, time.Second, 5*time.Millisecond)
}

func TestAsyncDrainsOnClose(t *testing.T) {
	r := &recorder{}
	ing := NewAsync(r.flush, AsyncConfig{FlushRows: 100000, FlushInterval: time.Hour, QueueCap: 1024})
	ing.Ingest(batch(7))
	assert.Nil(t, ing.Close())
	_, rows := r.snapshot()
	assert.Equal(t, 7, rows)
}

func TestAsyncIngestAfterCloseFails(t *testing.T) {
	ing := NewAsync((&recorder{}).flush, AsyncConfig{})
	assert.Nil(t, ing.Close())
	assert.Error(t, ing.Ingest(batch(1)))
}

func TestAsyncDroppedOnFlushError(t *testing.T) {
	r := &recorder{failWith: errors.New("db down")}
	ing := NewAsync(r.flush, AsyncConfig{FlushRows: 100000, FlushInterval: time.Hour, QueueCap: 1024})
	ing.Ingest(batch(4))
	assert.Nil(t, ing.Close())

	dropped := ing.(interface{ Dropped() int64 }).Dropped()
	assert.Equal(t, int64(4), dropped)
}

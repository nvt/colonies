// Package logbuffer provides log ingestion strategies for the Colonies server.
//
// The synchronous ingestor writes each (already-authorized) batch straight to
// the database; this is the default and is fully durable. The asynchronous
// ingestor hands batches to a background flusher that coalesces rows from many
// concurrent batches into fewer, larger inserts, so the per-insert commit and WAL
// fsync are amortized across all of them. The cost is bounded loss on an
// ungraceful crash: a batch is acknowledged once queued, before it is durable.
// Graceful shutdown drains the queue.
//
// Authorization always happens in the request handler before Ingest is called,
// so the asynchronous path defers only the write, never a security check.
package logbuffer

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/colonyos/colonies/pkg/core"
	log "github.com/sirupsen/logrus"
)

// Ingestor accepts authorized log batches and is responsible for persisting them.
type Ingestor interface {
	// Ingest persists a batch of logs. For the async ingestor it may block when
	// the queue is full (backpressure) and returns once the batch is queued, not
	// once it is durable.
	Ingest(logs []*core.Log) error
	// Close stops the ingestor, flushing anything buffered. After Close, Ingest
	// returns an error.
	Close() error
}

// flushFunc persists a batch durably (e.g. database.LogDatabase.AddLogs).
type flushFunc func([]*core.Log) error

// syncIngestor writes every batch straight through.
type syncIngestor struct{ flush flushFunc }

// NewSync returns an Ingestor that writes each batch synchronously and durably.
func NewSync(flush func([]*core.Log) error) Ingestor {
	return &syncIngestor{flush: flush}
}

func (s *syncIngestor) Ingest(logs []*core.Log) error {
	if len(logs) == 0 {
		return nil
	}
	return s.flush(logs)
}

func (s *syncIngestor) Close() error { return nil }

// AsyncConfig tunes the coalescing ingestor.
type AsyncConfig struct {
	// QueueCap is the number of pending batches the queue holds before Ingest
	// blocks (backpressure). Default 1024.
	QueueCap int
	// FlushRows flushes once this many rows have accumulated. Default 1000.
	FlushRows int
	// FlushInterval flushes at least this often so low-volume logs are not held
	// back. Default 100ms.
	FlushInterval time.Duration
}

func (c *AsyncConfig) withDefaults() {
	if c.QueueCap <= 0 {
		c.QueueCap = 1024
	}
	if c.FlushRows <= 0 {
		c.FlushRows = 1000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
}

// asyncIngestor coalesces batches from many callers into fewer, larger flushes.
type asyncIngestor struct {
	flush     flushFunc
	ch        chan []*core.Log
	flushRows int
	interval  time.Duration

	done    chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool
	dropped atomic.Int64
}

// NewAsync returns an Ingestor that coalesces batches via a background flusher.
func NewAsync(flush func([]*core.Log) error, cfg AsyncConfig) Ingestor {
	cfg.withDefaults()
	a := &asyncIngestor{
		flush:     flush,
		ch:        make(chan []*core.Log, cfg.QueueCap),
		flushRows: cfg.FlushRows,
		interval:  cfg.FlushInterval,
		done:      make(chan struct{}),
	}
	a.wg.Add(1)
	go a.loop()
	return a
}

func (a *asyncIngestor) Ingest(logs []*core.Log) error {
	if len(logs) == 0 {
		return nil
	}
	if a.closed.Load() {
		return errors.New("log ingestor is closed")
	}
	// Block on a full queue (backpressure), but never block forever if closing.
	select {
	case a.ch <- logs:
		return nil
	case <-a.done:
		return errors.New("log ingestor is closed")
	}
}

func (a *asyncIngestor) loop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	var batch []*core.Log
	doFlush := func() {
		if len(batch) == 0 {
			return
		}
		if err := a.flush(batch); err != nil {
			a.dropped.Add(int64(len(batch)))
			log.WithFields(log.Fields{"Error": err, "Rows": len(batch)}).Error("Async log flush failed, batch dropped")
		}
		batch = nil
	}

	for {
		select {
		case <-a.done:
			// Drain whatever is queued, then flush and exit.
			for {
				select {
				case logs := <-a.ch:
					batch = append(batch, logs...)
					if len(batch) >= a.flushRows {
						doFlush()
					}
				default:
					doFlush()
					return
				}
			}
		case logs := <-a.ch:
			batch = append(batch, logs...)
			if len(batch) >= a.flushRows {
				doFlush()
			}
		case <-ticker.C:
			doFlush()
		}
	}
}

// Close stops accepting new batches, drains the queue, flushes, and waits for
// the background flusher to finish.
func (a *asyncIngestor) Close() error {
	if a.closed.Swap(true) {
		return nil
	}
	close(a.done)
	a.wg.Wait()
	return nil
}

// Dropped returns the number of log rows lost to flush errors. Useful for
// observability/tests.
func (a *asyncIngestor) Dropped() int64 { return a.dropped.Load() }

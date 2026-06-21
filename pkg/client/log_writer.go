package client

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/colonyos/colonies/pkg/rpc"
)

// LogWriterOpts configures a LogWriter.
type LogWriterOpts struct {
	// FlushLines flushes the buffer once this many lines have accumulated
	// (default 64).
	FlushLines int
	// FlushInterval flushes the buffer at least this often, so low-volume logs
	// are not held back (default 250ms).
	FlushInterval time.Duration
}

var errLogWriterClosed = errors.New("log writer is closed")

// LogWriter buffers log lines and sends them to a process via AddLogs in batches,
// instead of one AddLog RPC per line. It implements io.Writer: write a process's
// stdout/stderr to it, and the bytes are split on '\n', buffered, and flushed on a
// size or time trigger, on Flush, or on Close.
//
// Create one per process with ColoniesClient.NewLogWriter, point the process's
// output at it, and call Close when the process finishes to flush remaining lines
// and stop the background flusher. It is safe for concurrent use.
//
// On a flush error the batch is dropped rather than buffered without limit, and
// the error is returned by the next Write, Flush, or Close.
type LogWriter struct {
	send       func([]rpc.LogEntry) error
	flushLines int

	bufMu    sync.Mutex
	buf      []rpc.LogEntry
	partial  []byte // trailing bytes with no newline yet
	flushErr error
	closed   bool

	flushMu sync.Mutex // serializes sends so batches are delivered in order

	ticker *time.Ticker
	done   chan struct{}
	wg     sync.WaitGroup
}

// NewLogWriter returns a LogWriter that batches log lines to the given process.
func (client *ColoniesClient) NewLogWriter(processID string, prvKey string, opts LogWriterOpts) *LogWriter {
	send := func(entries []rpc.LogEntry) error {
		return client.AddLogs(processID, entries, prvKey)
	}
	return newLogWriter(send, opts)
}

func newLogWriter(send func([]rpc.LogEntry) error, opts LogWriterOpts) *LogWriter {
	if opts.FlushLines <= 0 {
		opts.FlushLines = 64
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 250 * time.Millisecond
	}

	w := &LogWriter{
		send:       send,
		flushLines: opts.FlushLines,
		buf:        make([]rpc.LogEntry, 0, opts.FlushLines),
		done:       make(chan struct{}),
	}
	w.ticker = time.NewTicker(opts.FlushInterval)
	w.wg.Add(1)
	go w.loop()
	return w
}

func (w *LogWriter) loop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.done:
			return
		case <-w.ticker.C:
			w.flush()
		}
	}
}

// Write splits p into lines on '\n', buffering complete lines and holding any
// trailing partial line until the next Write or Close. It always reports len(p)
// consumed; a prior flush error (if any) is returned.
func (w *LogWriter) Write(p []byte) (int, error) {
	w.bufMu.Lock()
	if w.closed {
		w.bufMu.Unlock()
		return 0, errLogWriterClosed
	}

	data := append(w.partial, p...)
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		w.buf = append(w.buf, rpc.LogEntry{Timestamp: time.Now().UTC().UnixNano(), Message: string(data[:i])})
		data = data[i+1:]
	}
	w.partial = append([]byte(nil), data...) // keep trailing partial line (own copy)

	shouldFlush := len(w.buf) >= w.flushLines
	prevErr := w.flushErr
	w.bufMu.Unlock()

	if shouldFlush {
		w.flush()
	}
	return len(p), prevErr
}

// Flush sends any buffered complete lines immediately.
func (w *LogWriter) Flush() error {
	return w.flush()
}

func (w *LogWriter) flush() error {
	w.bufMu.Lock()
	if len(w.buf) == 0 {
		w.bufMu.Unlock()
		return nil
	}
	batch := w.buf
	w.buf = make([]rpc.LogEntry, 0, w.flushLines)
	w.bufMu.Unlock()

	w.flushMu.Lock()
	err := w.send(batch)
	w.flushMu.Unlock()

	if err != nil {
		w.bufMu.Lock()
		w.flushErr = err
		w.bufMu.Unlock()
	}
	return err
}

// Close flushes the remaining buffered lines (including any trailing partial
// line), stops the background flusher, and marks the writer closed. Subsequent
// Writes fail.
func (w *LogWriter) Close() error {
	w.bufMu.Lock()
	if w.closed {
		w.bufMu.Unlock()
		return nil
	}
	w.closed = true
	if len(w.partial) > 0 {
		w.buf = append(w.buf, rpc.LogEntry{Timestamp: time.Now().UTC().UnixNano(), Message: string(w.partial)})
		w.partial = nil
	}
	w.bufMu.Unlock()

	w.ticker.Stop()
	close(w.done)
	w.wg.Wait()
	return w.flush()
}

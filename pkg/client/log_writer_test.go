package client

import (
	"sync"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/rpc"
	"github.com/stretchr/testify/assert"
)

// fakeSink collects flushed entries in order.
type fakeSink struct {
	mu      sync.Mutex
	entries []rpc.LogEntry
	flushes int
}

func (s *fakeSink) send(entries []rpc.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
	s.flushes++
	return nil
}

func (s *fakeSink) messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.entries))
	for i, e := range s.entries {
		out[i] = e.Message
	}
	return out
}

func TestLogWriterFlushOnClose(t *testing.T) {
	sink := &fakeSink{}
	w := newLogWriter(sink.send, LogWriterOpts{FlushLines: 1000, FlushInterval: time.Hour})

	n, err := w.Write([]byte("line 1\nline 2\n"))
	assert.Nil(t, err)
	assert.Equal(t, 14, n)

	// Below the flush threshold and the timer is effectively disabled, so nothing
	// is sent until Close.
	assert.Empty(t, sink.messages())

	assert.Nil(t, w.Close())
	assert.Equal(t, []string{"line 1", "line 2"}, sink.messages())
}

func TestLogWriterFlushOnThreshold(t *testing.T) {
	sink := &fakeSink{}
	w := newLogWriter(sink.send, LogWriterOpts{FlushLines: 3, FlushInterval: time.Hour})
	defer w.Close()

	_, err := w.Write([]byte("a\nb\nc\n")) // 3 lines -> hits threshold -> flush
	assert.Nil(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, sink.messages())
}

func TestLogWriterPartialLineFlushedOnClose(t *testing.T) {
	sink := &fakeSink{}
	w := newLogWriter(sink.send, LogWriterOpts{FlushLines: 1000, FlushInterval: time.Hour})

	// Two writes that together form one line plus a trailing partial (no newline).
	w.Write([]byte("hello "))
	w.Write([]byte("world\ntrailing-no-newline"))
	assert.Nil(t, w.Close())

	assert.Equal(t, []string{"hello world", "trailing-no-newline"}, sink.messages())
}

func TestLogWriterTimerFlush(t *testing.T) {
	sink := &fakeSink{}
	w := newLogWriter(sink.send, LogWriterOpts{FlushLines: 1000, FlushInterval: 20 * time.Millisecond})
	defer w.Close()

	w.Write([]byte("tick\n"))
	assert.Eventually(t, func() bool {
		return len(sink.messages()) == 1
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"tick"}, sink.messages())
}

func TestLogWriterWriteAfterClose(t *testing.T) {
	sink := &fakeSink{}
	w := newLogWriter(sink.send, LogWriterOpts{})
	assert.Nil(t, w.Close())

	_, err := w.Write([]byte("nope\n"))
	assert.ErrorIs(t, err, errLogWriterClosed)
}

func TestLogWriterTimestampsSet(t *testing.T) {
	sink := &fakeSink{}
	w := newLogWriter(sink.send, LogWriterOpts{FlushLines: 1})
	w.Write([]byte("x\n"))
	w.Close()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	assert.NotEmpty(t, sink.entries)
	assert.Greater(t, sink.entries[0].Timestamp, int64(0))
}

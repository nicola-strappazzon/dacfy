package clickhouse

import (
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type finalizingLogger struct {
	completed bool
	finished  bool
}

func (l *finalizingLogger) WriteProgress(Progress) {}

func (l *finalizingLogger) FinishProgress() {
	l.finished = true
}

func (l *finalizingLogger) CompleteProgress() {
	l.completed = true
}

func TestFinishProgress(t *testing.T) {
	previous := loggerProgress
	t.Cleanup(func() { loggerProgress = previous })

	logger := &finalizingLogger{}
	loggerProgress = logger

	finishProgress()

	if !logger.finished {
		t.Fatal("expected progress logger to finish")
	}
}

func TestCompleteProgress(t *testing.T) {
	previous := loggerProgress
	t.Cleanup(func() { loggerProgress = previous })

	logger := &finalizingLogger{}
	loggerProgress = logger

	completeProgress()

	if !logger.completed {
		t.Fatal("expected progress logger to complete")
	}
}

// slowLogger records progress writes after a delay, like a slow terminal.
type slowLogger struct {
	mu     sync.Mutex
	writes []Progress
}

func (l *slowLogger) WriteProgress(in Progress) {
	time.Sleep(20 * time.Millisecond)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writes = append(l.writes, in)
}

func TestOnProgressFinishesWritesBeforeReturning(t *testing.T) {
	previous := loggerProgress
	t.Cleanup(func() { loggerProgress = previous })

	logger := &slowLogger{}
	loggerProgress = logger

	ch := &ClickHouse{}
	ch.Progress.StartNow()
	ch.onProgress(&clickhouse.Progress{Rows: 10, Bytes: 100, TotalRows: 20})
	ch.onProgress(&clickhouse.Progress{Rows: 5, Bytes: 50, TotalRows: 20})
	ch.Progress.StartNow()
	ch.onProgress(&clickhouse.Progress{Rows: 2, Bytes: 30, TotalRows: 8})

	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.writes) != 3 {
		t.Fatalf("expected 3 completed progress writes, got %d", len(logger.writes))
	}
	for i, want := range []Progress{
		{ReadRows: 10, ReadBytes: 100, TotalRows: 20},
		{ReadRows: 15, ReadBytes: 150, TotalRows: 20},
		{ReadRows: 2, ReadBytes: 30, TotalRows: 8},
	} {
		got := logger.writes[i]
		if got.ReadRows != want.ReadRows || got.ReadBytes != want.ReadBytes || got.TotalRows != want.TotalRows {
			t.Errorf("write %d: got rows=%d bytes=%d total=%d; want rows=%d bytes=%d total=%d", i, got.ReadRows, got.ReadBytes, got.TotalRows, want.ReadRows, want.ReadBytes, want.TotalRows)
		}
	}
}

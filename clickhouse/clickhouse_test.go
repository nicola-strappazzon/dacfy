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

func TestOnProgressWaitsForBackgroundWrites(t *testing.T) {
	previous := loggerProgress
	t.Cleanup(func() { loggerProgress = previous })

	logger := &slowLogger{}
	loggerProgress = logger

	ch := &ClickHouse{}
	var writes sync.WaitGroup
	ch.onProgress(&writes)(&clickhouse.Progress{Rows: 10, Bytes: 100, TotalRows: 20})
	writes.Wait()

	if len(logger.writes) != 1 {
		t.Fatalf("expected 1 progress write after wait, got %d", len(logger.writes))
	}
	if ch.Progress.ReadRows != 10 || ch.Progress.ReadBytes != 100 || ch.Progress.TotalRows != 20 {
		t.Fatalf("unexpected progress: %+v", ch.Progress)
	}
}

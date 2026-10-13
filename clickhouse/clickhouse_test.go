package clickhouse

import "testing"

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

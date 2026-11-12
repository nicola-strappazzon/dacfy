package task

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingExecutor counts runs and fails each one with err.
type countingExecutor struct {
	err  error
	runs chan string
}

func (c countingExecutor) Execute(statement string, _ bool) error {
	c.runs <- statement
	return c.err
}

// syncBuffer is a bytes.Buffer that the schedule goroutine and the test can
// use at the same time.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitRun(t *testing.T, runs chan string) string {
	t.Helper()
	select {
	case statement := <-runs:
		return statement
	case <-time.After(5 * time.Second):
		t.Fatal("task did not run on schedule")
		return ""
	}
}

func TestScheduleKeepsRunningAfterAFailedRun(t *testing.T) {
	executor := countingExecutor{err: errors.New("table is read only"), runs: make(chan string, 10)}
	out := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)

	go func() {
		done <- runner{out: out, pipeline: examplePipeline(), execute: executor}.Schedule(ctx, "sync", "@every 1s")
	}()

	assert.Equal(t, "SYSTEM SYNC REPLICA analytics.events", waitRun(t, executor.runs))
	waitRun(t, executor.runs)
	cancel()

	require.NoError(t, <-done)
	assert.Contains(t, out.String(), "--> Task sync scheduled with cron \"@every 1s\", press Ctrl+C to stop.\n")
	assert.Contains(t, out.String(), "--> Task sync failed: table is read only\n")
}

func TestScheduleRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name    string
		task    string
		spec    string
		message string
	}{
		{"invalid cron", "sync", "* * *", `invalid --cron "* * *": expected exactly 5 fields, found 3: [* * *]`},
		{"unknown task", "drop", "*/5 0 * * *", `task "drop" not found`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := countingExecutor{runs: make(chan string, 1)}
			err := runner{out: &bytes.Buffer{}, pipeline: examplePipeline(), execute: executor}.Schedule(context.Background(), tc.task, tc.spec)

			assert.EqualError(t, err, tc.message)
			assert.Empty(t, executor.runs)
		})
	}
}

func TestValidateCron(t *testing.T) {
	assert.NoError(t, validateCron(""))
	assert.NoError(t, validateCron("*/5 0 * * *"))
	assert.NoError(t, validateCron("@hourly"))
	assert.EqualError(t, validateCron("61 * * * *"), `invalid --cron "61 * * * *": end of range (61) above maximum (59): 61`)
}

package progress

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogHandlerWritesOneLinePerCompletedRun(t *testing.T) {
	var out bytes.Buffer
	handler := NewLogHandler(&out)
	p := clickhouse.Progress{
		Start: time.Now().Add(-2 * time.Second), ReadRows: 10, TotalRows: 100,
		ReadBytes: 1024, CPU: 5.5, Memory: 1024 * 1024,
	}
	handler.WriteProgress(p)
	p.ReadRows = 100
	handler.WriteProgress(p)
	assert.Empty(t, out.String())
	handler.CompleteProgress()

	p.StartNow()
	p.SetReadRows(5)
	handler.WriteProgress(p)
	handler.CompleteProgress()

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)
	for _, line := range lines {
		_, err := time.Parse(time.RFC3339, strings.Fields(line)[0])
		require.NoError(t, err)
		assert.NotContains(t, line, "%")
		assert.NotContains(t, line, " of ")
		assert.NotContains(t, line, "\r")
		assert.NotContains(t, line, "\x1b")
	}
	assert.Contains(t, lines[0], "100 Rows, 1 KiB, 5.50 CPU, 1 MiB RAM, Elapsed ")
	assert.Contains(t, lines[1], "5 Rows, 0, 0.00 CPU, 0 RAM")
}

func TestLogHandlerDoesNotReportFailedRunAsCompleted(t *testing.T) {
	var out bytes.Buffer
	handler := NewLogHandler(&out)
	handler.WriteProgress(clickhouse.Progress{Start: time.Now(), ReadRows: 100})
	handler.FinishProgress()
	assert.Empty(t, out.String())

	handler.WriteProgress(clickhouse.Progress{Start: time.Now(), ReadRows: 5})
	handler.CompleteProgress()
	assert.Contains(t, out.String(), "5 Rows")
	assert.NotContains(t, out.String(), "100 Rows")
}

package test

import (
	"fmt"
	"time"

	"github.com/nicola-strappazzon/dacfy/pipelines"
)

// measurement reports the rows and the client wall time of a test query.
type measurement struct {
	rows        *pipelines.RowCount
	maxDuration time.Duration
	count       uint64
	elapsed     time.Duration
}

func (m measurement) Outcomes() ([]outcome, error) {
	return []outcome{m.rowsOutcome(), m.durationOutcome()}, nil
}

func (m measurement) rowsOutcome() outcome {
	result := outcome{key: "rows", detail: fmt.Sprint(m.count), pass: true}
	if m.rows != nil {
		result.key += " " + m.rows.String()
		result.pass = m.rows.Match(m.count)
	}
	return result
}

func (m measurement) durationOutcome() outcome {
	result := outcome{key: "duration", detail: m.elapsed.Round(time.Millisecond).String(), pass: true}
	if m.maxDuration > 0 {
		result.key += " <= " + m.maxDuration.String()
		result.pass = m.elapsed <= m.maxDuration
	}
	return result
}

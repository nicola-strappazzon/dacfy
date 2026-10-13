package backfill

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
)

func TestRunChunkedTableQueryTruncateKeepsTableName(t *testing.T) {
	previousConfig := pl.Config
	previousFrom, previousTo, previousChunk := from, to, chunk
	previousTruncate := truncate
	t.Cleanup(func() {
		pl.Reset()
		pl.Config = previousConfig
		pl.SetParents()
		from, to, chunk = previousFrom, previousTo, previousChunk
		truncate = previousTruncate
	})

	pl.Reset()
	pl.Config = pipelines.Config{DryRun: true}
	pl.Database.Name = "analytics"
	pl.Table.Name = "daily_revenue"
	pl.Table.Query = "INSERT INTO daily_revenue SELECT day, sum(amount) FROM orders GROUP BY day"
	pl.Table.Backfill.Where = "day >= '{date_from}' AND day < '{date_to}'"
	pl.SetParents()

	from = "2026-01-01 00:00:00"
	to = "2026-01-01 01:00:00"
	chunk = "1h"
	truncate = true

	assert.NoError(t, runChunked())
	assert.Equal(t, pipelines.Name("daily_revenue"), pl.Table.Name)
}

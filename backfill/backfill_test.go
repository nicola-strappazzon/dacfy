package backfill

import (
	"io"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
)

func TestRunChunkedTableQueryTruncateKeepsTableName(t *testing.T) {
	previousConfig := pl.Config
	t.Cleanup(func() {
		pl.Reset()
		pl.Config = previousConfig
		pl.SetParents()
	})

	pl.Reset()
	pl.Config = pipelines.Config{DryRun: true}
	pl.Database.Name = "analytics"
	pl.Table.Name = "daily_revenue"
	pl.Table.Query = "INSERT INTO daily_revenue SELECT day, sum(amount) FROM orders GROUP BY day"
	pl.Table.Backfill.Where = "day >= '{date_from}' AND day < '{date_to}'"
	pl.SetParents()

	runner := NewRunner(io.Discard, pipelines.BackfillOptions{
		From:     "2026-01-01 00:00:00",
		To:       "2026-01-01 01:00:00",
		Chunk:    "1h",
		Truncate: true,
	})

	assert.NoError(t, runner.Run())
	assert.Equal(t, pipelines.Name("daily_revenue"), pl.Table.Name)
}

func TestRunnerValidate(t *testing.T) {
	cases := []struct {
		name    string
		options pipelines.BackfillOptions
		err     string
	}{
		{name: "full backfill", options: pipelines.BackfillOptions{}},
		{name: "chunk range", options: pipelines.BackfillOptions{From: "2026-01-01 00:00:00", To: "2026-01-02 00:00:00", Chunk: "1d"}},
		{name: "default chunk", options: pipelines.BackfillOptions{From: "2026-01-01 00:00:00", To: "2026-01-02 00:00:00"}},
		{name: "missing to", options: pipelines.BackfillOptions{From: "2026-01-01 00:00:00"}, err: "--from and --to must both be specified"},
		{name: "invalid from", options: pipelines.BackfillOptions{From: "2026-01-01", To: "2026-01-02 00:00:00"}, err: `--from: invalid format "2026-01-01", use YYYY-MM-DD HH:MM:SS`},
		{name: "from after to", options: pipelines.BackfillOptions{From: "2026-01-02 00:00:00", To: "2026-01-01 00:00:00"}, err: "--from must be before --to"},
		{name: "invalid chunk", options: pipelines.BackfillOptions{From: "2026-01-01 00:00:00", To: "2026-01-02 00:00:00", Chunk: "1w"}, err: `invalid --chunk "1w", use Nh (hours), Nd (days) or NM (months)`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewRunner(io.Discard, tc.options).Validate()
			if tc.err == "" {
				assert.NoError(t, err)
				return
			}

			assert.EqualError(t, err, tc.err)
		})
	}
}

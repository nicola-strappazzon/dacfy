package create_test

import (
	"bytes"
	"testing"

	"github.com/nicola-strappazzon/dacfy/create"
	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
)

func load(in string) {
	pl := pipelines.Instance()
	pl.Config.Pipe = in
	pl.Config.SQL = true
	pl.Config.DryRun = true
	pl.Reset()
	pl.Load()
	pl.SetParents()
}

func TestCommand(t *testing.T) {
	cases := []struct {
		PipeFile     string
		ExactMatch   []string
		PartialMatch []string
	}{
		{
			PipeFile: "../examples/wikistat/table.yaml",
			ExactMatch: []string{
				"--> Create database: wikistat",
				"CREATE DATABASE IF NOT EXISTS wikistat;",
				"USE wikistat;",
				"--> Create table: wikistat",
			},
			PartialMatch: []string{
				`CREATE TABLE IF NOT EXISTS wikistat.wikistat .*;`,
			},
		},
		{
			PipeFile: "../examples/wikistat/view.yaml",
			ExactMatch: []string{
				"--> Create database: wikistat",
				"CREATE DATABASE IF NOT EXISTS wikistat;",
				"USE wikistat;",
				"--> Create table: wikistat_top_projects",
			},
			PartialMatch: []string{
				`CREATE MATERIALIZED VIEW IF NOT EXISTS wikistat.wikistat_top_projects_mv TO wikistat.wikistat_top_projects AS SELECT .*;`,
			},
		},
		{
			PipeFile: "../examples/download/view.yaml",
			ExactMatch: []string{
				"--> Create database: download",
				"CREATE DATABASE IF NOT EXISTS download;",
				"USE download;",
				"--> Create view: download_daily_mv",
			},
			PartialMatch: []string{
				`CREATE VIEW IF NOT EXISTS download.download_daily_mv AS SELECT .*;`,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.PipeFile, func(t *testing.T) {
			var buf bytes.Buffer

			load(tc.PipeFile)

			cmd := create.NewCommand()
			cmd.SetOut(&buf)
			cmd.SetErr(&buf)

			err := cmd.Execute()
			out := buf.String()

			assert.NoError(t, err)

			for _, substring := range tc.ExactMatch {
				assert.Contains(t, out, substring)
			}

			for _, regex := range tc.PartialMatch {
				assert.Regexp(t, regex, out)
			}
		})
	}
}

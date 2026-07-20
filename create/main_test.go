package create_test

import (
	"bytes"
	"os"
	"path/filepath"
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
		NotMatch     []string
	}{
		{
			PipeFile: "../examples/wikistat/table.yaml",
			ExactMatch: []string{
				"USE wikistat;",
			},
			PartialMatch: []string{
				`CREATE TABLE IF NOT EXISTS wikistat.wikistat .*;`,
			},
			NotMatch: []string{
				"--> Create database: wikistat",
				"CREATE DATABASE IF NOT EXISTS wikistat;",
			},
		},
		{
			PipeFile: "../examples/wikistat/view.yaml",
			ExactMatch: []string{
				"USE wikistat;",
			},
			PartialMatch: []string{
				`CREATE MATERIALIZED VIEW IF NOT EXISTS wikistat.wikistat_top_projects_mv TO wikistat.wikistat_top_projects AS SELECT .*;`,
			},
			NotMatch: []string{
				"--> Create database: wikistat",
				"CREATE DATABASE IF NOT EXISTS wikistat;",
			},
		},
		{
			PipeFile: "../examples/download/view.yaml",
			ExactMatch: []string{
				"USE download;",
			},
			PartialMatch: []string{
				`CREATE VIEW IF NOT EXISTS download.download_daily_mv AS SELECT .*;`,
			},
			NotMatch: []string{
				"--> Create database: download",
				"CREATE DATABASE IF NOT EXISTS download;",
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

			for _, substring := range tc.NotMatch {
				assert.NotContains(t, out, substring)
			}
		})
	}
}

func TestCommand_DatabaseOnly(t *testing.T) {
	pipe := filepath.Join(t.TempDir(), "database.yaml")
	err := os.WriteFile(pipe, []byte(`---
database:
  name: malware_search
  cluster: zynap_prd
  replicated:
    path: /clickhouse/databases/malware_search
    replica: "{replica}"
`), 0600)
	assert.NoError(t, err)

	var buf bytes.Buffer

	load(pipe)

	cmd := create.NewCommand()
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)

	err = cmd.Execute()
	out := buf.String()

	assert.NoError(t, err)
	assert.NotContains(t, out, "-->")
	assert.Contains(t, out, "CREATE DATABASE IF NOT EXISTS malware_search ON CLUSTER zynap_prd ENGINE = Replicated('/clickhouse/databases/malware_search', '{replica}');")
	assert.Contains(t, out, "USE malware_search;")
}

func TestCommand_UserWithGrants(t *testing.T) {
	pipe := filepath.Join(t.TempDir(), "user.yaml")
	err := os.WriteFile(pipe, []byte(`---
database:
  name: malware_search
  cluster: zynap_prd
user:
  name: malware
  password: 'EWZJcEvRZg9zfsg1'
  cluster: zynap_prd
  grants:
    - { privilege: SHOW,   on: malware_search.* }
    - { privilege: SELECT, on: malware_search.* }
`), 0600)
	assert.NoError(t, err)

	var buf bytes.Buffer

	load(pipe)

	cmd := create.NewCommand()
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)

	err = cmd.Execute()
	out := buf.String()

	assert.NoError(t, err)
	assert.NotContains(t, out, "CREATE DATABASE")
	assert.Contains(t, out, "CREATE USER IF NOT EXISTS malware ON CLUSTER zynap_prd IDENTIFIED BY 'EWZJcEvRZg9zfsg1';")
	assert.Contains(t, out, "GRANT ON CLUSTER zynap_prd SHOW ON malware_search.* TO malware;")
	assert.Contains(t, out, "GRANT ON CLUSTER zynap_prd SELECT ON malware_search.* TO malware;")
}

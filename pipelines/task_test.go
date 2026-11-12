package pipelines_test

import (
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskSQLFromYAML(t *testing.T) {
	var p pipelines.Pipelines
	require.NoError(t, yaml.Unmarshal([]byte(`tasks:
  - name: sync
    query: SYSTEM SYNC REPLICA {table};
  - name: kill
    query: |
      KILL QUERY WHERE user = 'etl'
`), &p))

	sync, err := p.Tasks.Find("sync")
	require.NoError(t, err)
	statement, err := sync.SQL("analytics.events_v2")
	require.NoError(t, err)
	assert.Equal(t, "SYSTEM SYNC REPLICA analytics.events_v2", statement)

	kill, err := p.Tasks.Find("kill")
	require.NoError(t, err)
	statement, err = kill.SQL("")
	require.NoError(t, err)
	assert.Equal(t, "KILL QUERY WHERE user = 'etl'", statement)
}

func TestTaskSQLWithoutTable(t *testing.T) {
	_, err := pipelines.Task{Name: "sync", Query: "SYSTEM SYNC REPLICA {table}"}.SQL("")
	assert.EqualError(t, err, `task "sync" uses {table} but table.name is not set`)
}

func TestTasksFind(t *testing.T) {
	valid := pipelines.Task{Name: "sync", Query: "SYSTEM SYNC REPLICA {table}"}
	cases := []struct {
		name    string
		tasks   pipelines.Tasks
		find    string
		message string
	}{
		{"found", pipelines.Tasks{valid}, "sync", ""},
		{"no tasks", nil, "sync", "no tasks defined"},
		{"missing name", pipelines.Tasks{{Query: "SELECT 1"}}, "sync", `task.name "" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`},
		{"invalid name", pipelines.Tasks{{Name: "1sync", Query: "SELECT 1"}}, "1sync", `task.name "1sync" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`},
		{"missing query", pipelines.Tasks{{Name: "sync"}}, "sync", `task.query is required for task "sync"`},
		{"repeated name", pipelines.Tasks{valid, valid}, "sync", `task.name "sync" is repeated`},
		{"unknown task", pipelines.Tasks{valid}, "optimize", `task "optimize" not found`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item, err := tc.tasks.Find(tc.find)
			if tc.message == "" {
				require.NoError(t, err)
				assert.Equal(t, valid, item)
				return
			}
			assert.EqualError(t, err, tc.message)
		})
	}
}

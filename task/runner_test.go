package task

import (
	"bytes"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeExecutor struct {
	statements []string
}

func (f *fakeExecutor) Execute(statement string, _ bool) error {
	f.statements = append(f.statements, statement)
	return nil
}

func examplePipeline() *pipelines.Pipelines {
	return &pipelines.Pipelines{
		Database: pipelines.Database{Name: "analytics"},
		Table:    pipelines.Table{Name: "events"},
		Tasks: pipelines.Tasks{
			{Name: "sync", Query: "SYSTEM SYNC REPLICA {table}"},
			{Name: "optimize", Query: "OPTIMIZE TABLE {table}\n  FINAL"},
		},
	}
}

func TestRunExecutesOnlyTheNamedTask(t *testing.T) {
	pipeline := examplePipeline()
	pipeline.Config.Suffix = "_v2"
	pipeline.Config.SQL = true
	executor := &fakeExecutor{}
	var out bytes.Buffer

	require.NoError(t, runner{out: &out, pipeline: pipeline, execute: executor}.Run("sync"))

	assert.Equal(t, []string{"SYSTEM SYNC REPLICA analytics.events_v2"}, executor.statements)
	assert.Equal(t, "--> Running task: sync\nSYSTEM SYNC REPLICA analytics.events_v2;\n", out.String())
}

func TestRunDryRunDoesNotExecute(t *testing.T) {
	pipeline := examplePipeline()
	pipeline.Config.DryRun = true
	pipeline.Config.SQL = true
	executor := &fakeExecutor{}
	var out bytes.Buffer

	require.NoError(t, runner{out: &out, pipeline: pipeline, execute: executor}.Run("sync"))

	assert.Empty(t, executor.statements)
	assert.Equal(t, "SYSTEM SYNC REPLICA analytics.events;\n", out.String())
}

func TestRunUnknownTask(t *testing.T) {
	executor := &fakeExecutor{}
	err := runner{out: &bytes.Buffer{}, pipeline: examplePipeline(), execute: executor}.Run("drop")

	assert.EqualError(t, err, `task "drop" not found`)
	assert.Empty(t, executor.statements)
}

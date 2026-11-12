package tasks

import (
	"bytes"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	var out bytes.Buffer
	items := pipelines.Tasks{
		{Name: "sync", Query: "SYSTEM SYNC REPLICA {table}"},
		{Name: "optimize", Query: "OPTIMIZE TABLE {table}\n  FINAL"},
	}

	require.NoError(t, lister{out: &out, tasks: items}.List())

	assert.Equal(t, "sync      SYSTEM SYNC REPLICA {table}\noptimize  OPTIMIZE TABLE {table} FINAL\n", out.String())
}

func TestListWithoutTasks(t *testing.T) {
	err := lister{out: &bytes.Buffer{}}.List()

	assert.EqualError(t, err, "no tasks defined")
}

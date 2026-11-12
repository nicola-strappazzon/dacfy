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
		{Name: "sync", Description: "Wait for the replica to get all parts.", Query: "SYSTEM SYNC REPLICA {table}"},
		{Name: "optimize", Description: "Merge all parts.", Query: "OPTIMIZE TABLE {table} FINAL"},
	}

	require.NoError(t, lister{out: &out, tasks: items}.List())

	assert.Equal(t, "sync      Wait for the replica to get all parts.\noptimize  Merge all parts.\n", out.String())
}

func TestListWithoutTasks(t *testing.T) {
	err := lister{out: &bytes.Buffer{}}.List()

	assert.EqualError(t, err, "no tasks defined")
}

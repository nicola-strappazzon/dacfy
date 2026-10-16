package test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
)

func examplePipeline(tests []pipelines.Test) *pipelines.Pipelines {
	return &pipelines.Pipelines{
		Database: pipelines.Database{Name: "analytics"},
		Tests:    tests,
	}
}

func exampleTest() pipelines.Test {
	return pipelines.Test{
		Name:  "daily_revenue_examples",
		Table: "daily_revenue",
		Key:   []string{"id"},
		Expected: []map[string]any{
			{"id": 101, "revenue": 25},
			{"id": 205, "revenue": 7},
		},
	}
}

func exampleResult(rows ...[]any) clickhouse.Result {
	values := make([]clickhouse.Values, len(rows))
	for i, row := range rows {
		values[i] = make(clickhouse.Values, len(row))
		for j, value := range row {
			values[i][j] = clickhouse.NewValue(value)
		}
	}

	return clickhouse.Result{
		Columns: []clickhouse.Column{{Name: "id"}, {Name: "revenue"}},
		Rows:    values,
	}
}

type queryFunc func(string) (clickhouse.Result, error)

func (f queryFunc) Query(statement string) (clickhouse.Result, error) {
	return f(statement)
}

func TestRunnerReportsEachExpectedRow(t *testing.T) {
	var out bytes.Buffer
	var statement string
	r := runner{
		out:      &out,
		pipeline: examplePipeline([]pipelines.Test{exampleTest()}),
		query: queryFunc(func(in string) (clickhouse.Result, error) {
			statement = in
			return exampleResult([]any{uint64(101), 25}, []any{uint64(205), 6}), nil
		}),
	}

	assert.EqualError(t, r.Run(), `test "daily_revenue_examples" failed`)
	assert.Equal(t, "SELECT id, revenue FROM analytics.daily_revenue WHERE id IN (101, 205)", statement)
	assert.Equal(t, "--> Test: daily_revenue_examples\n    id=101: PASS\n    id=205: FAIL (values differ)\n", out.String())
}

func TestRunnerPassesWhenRowsMatch(t *testing.T) {
	var out bytes.Buffer
	r := runner{
		out:      &out,
		pipeline: examplePipeline([]pipelines.Test{exampleTest()}),
		query: queryFunc(func(string) (clickhouse.Result, error) {
			return exampleResult([]any{uint64(101), 25}, []any{uint64(205), 7}), nil
		}),
	}

	assert.NoError(t, r.Run())
	assert.Contains(t, out.String(), "id=101: PASS")
	assert.Contains(t, out.String(), "id=205: PASS")
}

func TestRunnerReturnsQueryError(t *testing.T) {
	var out bytes.Buffer
	r := runner{
		out:      &out,
		pipeline: examplePipeline([]pipelines.Test{exampleTest()}),
		query: queryFunc(func(string) (clickhouse.Result, error) {
			return clickhouse.Result{}, errors.New("boom")
		}),
	}

	assert.EqualError(t, r.Run(), `test "daily_revenue_examples": boom`)
	assert.Equal(t, "--> Test error: daily_revenue_examples\n", out.String())
}

func TestRunnerDryRunDoesNotQueryClickHouse(t *testing.T) {
	var out bytes.Buffer
	pipeline := examplePipeline([]pipelines.Test{exampleTest()})
	pipeline.Config.DryRun = true
	r := runner{
		out:      &out,
		pipeline: pipeline,
		query: queryFunc(func(string) (clickhouse.Result, error) {
			t.Fatal("query must not run during a dry run")
			return clickhouse.Result{}, nil
		}),
	}

	assert.NoError(t, r.Run())
	assert.Contains(t, out.String(), "SELECT id, revenue FROM analytics.daily_revenue WHERE id IN (101, 205);")
}

func TestRunnerRejectsEmptyTestList(t *testing.T) {
	r := runner{out: &bytes.Buffer{}, pipeline: examplePipeline(nil)}
	assert.EqualError(t, r.Run(), "no tests defined")
}

func TestCommandRequiresYAMLFile(t *testing.T) {
	cmd := NewCommand()
	assert.EqualError(t, cmd.Args(cmd, nil), "missing YAML file")
}

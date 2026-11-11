package test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunnerUnique(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		var out bytes.Buffer
		r := runner{
			out:      &out,
			pipeline: examplePipeline([]pipelines.Test{{Name: "unique_ids", Table: "events", Final: true, Unique: []string{"id"}}}),
			query: queryFunc(func(sql string) (clickhouse.Result, error) {
				assert.Equal(t, "SELECT id, count() FROM analytics.events FINAL GROUP BY id HAVING count() > 1 ORDER BY id", sql)
				if duplicate {
					return exampleResult([]any{101, uint64(2)}), nil
				}
				return exampleResult(), nil
			}),
		}
		if duplicate {
			assert.EqualError(t, r.Run(), `test "unique_ids" failed`)
			assert.Contains(t, out.String(), "id=101: FAIL (found 2 rows)")
		} else {
			assert.NoError(t, r.Run())
			assert.Contains(t, out.String(), "unique (id): PASS")
		}
	}
}

func TestRunnerChecksReportsAllRules(t *testing.T) {
	var out bytes.Buffer
	r := runner{
		out: &out,
		pipeline: examplePipeline([]pipelines.Test{{Name: "quality", Table: "events", Checks: []pipelines.Check{
			{Field: "id", NotNull: true},
			{Field: "score", Between: []float64{0, 100}},
			{Field: "size", NotNull: true},
		}}}),
		query: queryFunc(func(string) (clickhouse.Result, error) {
			return exampleResult([]any{uint64(0), uint64(3), uint64(0)}), nil
		}),
	}
	assert.EqualError(t, r.Run(), `test "quality" failed`)
	assert.Equal(t, "--> Test: quality\n    id not_null: PASS\n    score between [0, 100]: FAIL (3 rows)\n    size not_null: PASS\n", out.String())
}

func TestChecksEmptyResultIsAnError(t *testing.T) {
	_, err := (checks{rules: []pipelines.Check{{Field: "id", NotNull: true}}, result: exampleResult()}).Outcomes()
	require.Error(t, err)
}

func TestRunnerChecksPass(t *testing.T) {
	var out bytes.Buffer
	r := runner{
		out:      &out,
		pipeline: examplePipeline([]pipelines.Test{{Name: "quality", Table: "events", Checks: []pipelines.Check{{Field: "id", NotNull: true}}}}),
		query: queryFunc(func(string) (clickhouse.Result, error) {
			return exampleResult([]any{uint64(0)}), nil
		}),
	}
	assert.NoError(t, r.Run())
	assert.Contains(t, out.String(), "id not_null: PASS")
}

func TestUniquenessCompositeNullKey(t *testing.T) {
	item := uniqueness{fields: key{"source", "id"}, result: exampleResult([]any{"vendor_a", nil, uint64(2)})}
	outcomes, err := item.Outcomes()
	require.NoError(t, err)
	assert.Equal(t, []outcome{{key: "source=vendor_a, id=<null>", detail: "found 2 rows"}}, outcomes)
}

func TestRunnerQueryReportsRowsAndDuration(t *testing.T) {
	one, three := uint64(1), uint64(3)
	cases := []struct {
		name  string
		item  pipelines.Test
		sleep time.Duration
		lines []string
		err   string
	}{
		{"report only", pipelines.Test{}, 0, []string{"    rows: PASS (2)", "    duration: PASS ("}, ""},
		{"rows pass", pipelines.Test{Rows: &pipelines.RowCount{GTE: &one, LTE: &three}}, 0, []string{"    rows between [1, 3]: PASS (2)"}, ""},
		{"rows fail", pipelines.Test{Rows: &pipelines.RowCount{EQ: &three}}, 0, []string{"    rows = 3: FAIL (2)"}, `test "lookup" failed`},
		{"duration pass", pipelines.Test{MaxDuration: time.Minute}, 0, []string{"    duration <= 1m0s: PASS ("}, ""},
		{"duration fail", pipelines.Test{MaxDuration: time.Millisecond}, 20 * time.Millisecond, []string{"    duration <= 1ms: FAIL ("}, `test "lookup" failed`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item := tc.item
			item.Name, item.Table, item.Query = "lookup", "events", "SELECT id FROM {table} WHERE id > 100"
			pipeline := examplePipeline([]pipelines.Test{item})
			pipeline.Config.Suffix = "_v7"
			var out bytes.Buffer
			r := runner{out: &out, pipeline: pipeline, query: queryFunc(func(sql string) (clickhouse.Result, error) {
				assert.Equal(t, "SELECT id FROM analytics.events_v7 WHERE id > 100", sql)
				time.Sleep(tc.sleep)
				return exampleResult([]any{101}, []any{205}), nil
			})}
			err := r.Run()
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.err)
			}
			assert.True(t, strings.HasPrefix(out.String(), "--> Test: lookup\n"))
			for _, line := range tc.lines {
				assert.Contains(t, out.String(), line)
			}
		})
	}
}

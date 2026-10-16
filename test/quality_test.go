package test

import (
	"bytes"
	"testing"

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
			pipeline: examplePipeline([]pipelines.Test{{Name: "unique_ids", Table: "events", Unique: []string{"id"}}}),
			query: queryFunc(func(sql string) (clickhouse.Result, error) {
				assert.Equal(t, "SELECT id, count() FROM analytics.events GROUP BY id HAVING count() > 1 ORDER BY id", sql)
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

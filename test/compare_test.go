package test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComparisonOutcomes(t *testing.T) {
	item := comparison{
		pipeline: pipelines.Test{
			Key: []string{"id"},
			Expected: []map[string]any{
				{"id": 1, "name": "dacfy"},
			},
		},
		result: clickhouse.Result{
			Columns: []clickhouse.Column{{Name: "id"}, {Name: "name"}},
			Rows:    []clickhouse.Values{{clickhouse.NewValue(uint64(1)), clickhouse.NewValue("dacfy")}},
		},
	}

	outcomes, err := item.Outcomes()

	assert.NoError(t, err)
	assert.Equal(t, []outcome{{key: "id=1", pass: true}}, outcomes)
}

func TestComparisonFailures(t *testing.T) {
	cases := []struct {
		name   string
		rows   [][]any
		detail string
	}{
		{"missing", nil, "missing"},
		{"different", [][]any{{1, 99}}, "values differ"},
		{"duplicate", [][]any{{1, 25}, {1, 25}}, "found 2 rows"},
		{"null is not text", [][]any{{1, "<null>"}}, "values differ"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expected := map[string]any{"id": 1, "revenue": 25}
			if tc.name == "null is not text" {
				expected["revenue"] = nil
			}
			item := comparison{
				pipeline: pipelines.Test{Key: []string{"id"}, Expected: []map[string]any{expected}},
				result:   exampleResult(tc.rows...),
			}
			outcomes, err := item.Outcomes()
			require.NoError(t, err)
			assert.Equal(t, []outcome{{key: "id=1", detail: tc.detail}}, outcomes)
		})
	}
}

func TestComparisonPreservesManifestOrder(t *testing.T) {
	item := comparison{
		pipeline: pipelines.Test{Key: []string{"id"}, Expected: []map[string]any{{"id": 2, "revenue": 7}, {"id": 1, "revenue": 25}}},
		result:   exampleResult([]any{1, 25}, []any{2, 7}, []any{3, 8}),
	}
	outcomes, err := item.Outcomes()
	require.NoError(t, err)
	assert.Equal(t, []outcome{
		{key: "id=2", pass: true},
		{key: "id=1", pass: true},
		{key: `"3"`, detail: "unexpected row"},
	}, outcomes)
}

func TestComparisonRejectsEquivalentExpectedKeys(t *testing.T) {
	item := comparison{
		pipeline: pipelines.Test{Key: []string{"id"}, Expected: []map[string]any{{"id": 1}, {"id": "1"}}},
		result:   exampleResult(),
	}
	_, err := item.Outcomes()
	assert.EqualError(t, err, "test.expected contains duplicate key values")
}

func TestComparisonLists(t *testing.T) {
	text := func(in string) *string { return &in }
	cases := []struct {
		name     string
		expected any
		actual   any
		pass     bool
	}{
		{"strings", []any{"a", "b"}, []string{"a", "b"}, true},
		{"empty", []any{}, []string{}, true},
		{"empty and nil", []any{}, []string(nil), true},
		{"numbers", []any{uint64(1), uint64(2)}, []int64{1, 2}, true},
		{"nested", []any{[]any{uint64(1)}, []any{}}, [][]int64{{1}, {}}, true},
		{"null item", []any{"a", nil}, []*string{text("a"), nil}, true},
		{"null is not text", []any{nil}, []*string{text("<null>")}, false},
		{"different length", []any{"a"}, []string{"a", "b"}, false},
		{"items with spaces", []any{"a b", "c"}, []string{"a", "b c"}, false},
		{"list and text", []any{"a"}, "[a]", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item := comparison{
				pipeline: pipelines.Test{Key: []string{"id"}, Expected: []map[string]any{{"id": 1, "tags": tc.expected}}},
				result: clickhouse.Result{
					Columns: []clickhouse.Column{{Name: "id"}, {Name: "tags"}},
					Rows:    []clickhouse.Values{{clickhouse.NewValue(uint64(1)), clickhouse.NewValue(tc.actual)}},
				},
			}
			outcomes, err := item.Outcomes()
			require.NoError(t, err)
			want := outcome{key: "id=1", pass: true}
			if !tc.pass {
				want = outcome{key: "id=1", detail: "values differ"}
			}
			assert.Equal(t, []outcome{want}, outcomes)
		})
	}
}

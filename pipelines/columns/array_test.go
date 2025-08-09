package columns_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines/columns"

	"github.com/stretchr/testify/assert"
)

func TestArray_IsEmpty_And_IsNotEmpty(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		var arr columns.Array
		assert.True(t, arr.IsEmpty())
		assert.False(t, arr.IsNotEmpty())
	})

	t.Run("non-empty array", func(t *testing.T) {
		arr := columns.Array{
			columns.Name("a"),
		}
		assert.False(t, arr.IsEmpty())
		assert.True(t, arr.IsNotEmpty())
	})
}

func TestArray_ToArrayString(t *testing.T) {
	cases := []struct {
		name     string
		input    columns.Array
		expected []string
	}{
		{
			name:     "empty",
			input:    columns.Array{},
			expected: []string{},
		}, {
			name:     "single",
			input:    columns.Array{columns.Name("a")},
			expected: []string{"a"},
		}, {
			name: "multiple preserve order",
			input: columns.Array{
				columns.Name("a"),
				columns.Name("b"),
				columns.Name("c"),
			},
			expected: []string{"a", "b", "c"},
		}, {
			name: "with function-like names",
			input: columns.Array{
				columns.Name("AggregateFunction(avg, Float64)"),
				columns.Name("AggregateFunction(count)"),
			},
			expected: []string{
				"AggregateFunction(avg, Float64)",
				"AggregateFunction(count)",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.ToArrayString()
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestArray_Join(t *testing.T) {
	cases := []struct {
		name     string
		input    columns.Array
		expected string
	}{
		{
			name:     "empty",
			input:    columns.Array{},
			expected: "",
		}, {
			name:     "single",
			input:    columns.Array{columns.Name("a")},
			expected: "a",
		}, {
			name: "multiple preserve order",
			input: columns.Array{
				columns.Name("a"),
				columns.Name("b"),
				columns.Name("c"),
			},
			expected: "a,b,c",
		}, {
			name: "with function-like names",
			input: columns.Array{
				columns.Name("AggregateFunction(avg, Float64)"),
				columns.Name("AggregateFunction(count)"),
			},
			expected: "AggregateFunction(avg, Float64),AggregateFunction(count)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.Join()
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestArray_NotIn(t *testing.T) {
	cases := []struct {
		name     string
		left     columns.Array
		right    columns.Array
		expected []string
		ok       bool
	}{
		{
			name:     "both empty",
			left:     columns.Array{},
			right:    columns.Array{},
			expected: []string{},
			ok:       false,
		},
		{
			name:     "left empty",
			left:     columns.Array{},
			right:    columns.Array{columns.Name("id")},
			expected: []string{},
			ok:       false,
		},
		{
			name:     "right empty -> all from left",
			left:     columns.Array{columns.Name("id"), columns.Name("name")},
			right:    columns.Array{},
			expected: []string{"id", "name"},
			ok:       true,
		},
		{
			name:     "some overlap",
			left:     columns.Array{columns.Name("id"), columns.Name("name"), columns.Name("email")},
			right:    columns.Array{columns.Name("id"), columns.Name("name")},
			expected: []string{"email"},
			ok:       true,
		},
		{
			name:     "all contained",
			left:     columns.Array{columns.Name("id"), columns.Name("name")},
			right:    columns.Array{columns.Name("name"), columns.Name("id"), columns.Name("created_at")},
			expected: []string{},
			ok:       false,
		},
		{
			name:     "duplicates preserved and order kept",
			left:     columns.Array{columns.Name("a"), columns.Name("b"), columns.Name("b"), columns.Name("c")},
			right:    columns.Array{columns.Name("a"), columns.Name("x")},
			expected: []string{"b", "b", "c"},
			ok:       true,
		},
		{
			name:     "function-like names compare and find one mismatch",
			left:     columns.Array{columns.Name("toYYYYMM(event_date)")},
			right:    columns.Array{columns.Name("event_day"), columns.Name("user_id")},
			expected: []string{"event_date"},
			ok:       true,
		},
		{
			name:     "function-like names compare and all contained",
			left:     columns.Array{columns.Name("toYYYYMM(event_date)")},
			right:    columns.Array{columns.Name("event_date"), columns.Name("user_id")},
			expected: []string{},
			ok:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diff, ok := tc.left.NotIn(tc.right)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.expected, diff)
		})
	}
}

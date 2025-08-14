package columns_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines/columns"

	"github.com/stretchr/testify/assert"
)

func TestMap_IsEmpty_And_IsNotEmpty(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		var m columns.Map
		assert.True(t, m.IsEmpty())
		assert.False(t, m.IsNotEmpty())
	})

	t.Run("non-empty map", func(t *testing.T) {
		m := columns.Map{
			{Name: "a", Type: "Int32"},
		}
		assert.False(t, m.IsEmpty())
		assert.True(t, m.IsNotEmpty())
	})
}

func TestMap_JoinWithoutTypes(t *testing.T) {
	cases := []struct {
		name     string
		input    columns.Map
		expected string
	}{
		{
			name:     "empty",
			input:    columns.Map{},
			expected: "",
		}, {
			name: "single",
			input: columns.Map{
				{Name: "a", Type: "Int32"},
			},
			expected: "a",
		}, {
			name: "multiple preserve order",
			input: columns.Map{
				{Name: "a", Type: "Int32"},
				{Name: "b", Type: "String"},
				{Name: "c", Type: "DateTime"},
			},
			expected: "a,b,c",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.JoinWithoutTypes()
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestMap_WithTypes(t *testing.T) {
	cases := []struct {
		name     string
		input    columns.Map
		expected string
	}{
		{
			name:     "empty",
			input:    columns.Map{},
			expected: "",
		}, {
			name: "single",
			input: columns.Map{
				{Name: "a", Type: "Int32"},
			},
			expected: "a Int32",
		}, {
			name: "multiple preserve order",
			input: columns.Map{
				{Name: "a", Type: "Int32"},
				{Name: "b", Type: "String"},
				{Name: "c", Type: "DateTime"},
			},
			expected: "a Int32,b String,c DateTime",
		}, {
			name: "with aggregate functions",
			input: columns.Map{
				{Name: "col1", Type: "AggregateFunction(avg, Float64)"},
				{Name: "col2", Type: "AggregateFunction(count)"},
			},
			expected: "col1 AggregateFunction(avg, Float64),col2 AggregateFunction(count)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.JoinWithTypes()
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestMap_ToArray(t *testing.T) {
	cases := []struct {
		name     string
		in       columns.Map
		expected columns.Array
	}{
		{
			name:     "empty map -> empty array",
			in:       columns.Map{},
			expected: columns.Array(nil),
		},
		{
			name: "single column",
			in: columns.Map{
				{Name: "user_id", Type: "UInt64"},
			},
			expected: columns.Array{columns.Name("user_id")},
		},
		{
			name: "multiple preserve order",
			in: columns.Map{
				{Name: "event_time", Type: "DateTime"},
				{Name: "user_id", Type: "UInt64"},
				{Name: "country_code", Type: "FixedString(2)"},
			},
			expected: columns.Array{
				columns.Name("event_time"),
				columns.Name("user_id"),
				columns.Name("country_code"),
			},
		},
		{
			name: "function-like types ignored in names",
			in: columns.Map{
				{Name: "avg_session", Type: "AggregateFunction(avg, Float64)"},
				{Name: "cnt", Type: "AggregateFunction(count)"},
			},
			expected: columns.Array{
				columns.Name("avg_session"),
				columns.Name("cnt"),
			},
		},
		{
			name: "partition-style names",
			in: columns.Map{
				{Name: "toYYYYMM(event_time)", Type: "UInt32"},
				{Name: "toDate(event_time)", Type: "Date"},
			},
			expected: columns.Array{
				columns.Name("toYYYYMM(event_time)"),
				columns.Name("toDate(event_time)"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in.ToArray()
			assert.Equal(t, tc.expected, got)
		})
	}
}

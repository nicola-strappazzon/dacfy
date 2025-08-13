package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestQuery_ToString(t *testing.T) {
	cases := []struct {
		name     string
		input    pipelines.Query
		expected string
	}{
		{"empty", "", ""},
		{"plain", "SELECT 1", "SELECT 1"},
		{"trim spaces", "   SELECT 1   ", "SELECT 1"},
		{"trim newlines/tabs", "\n\t  SELECT 1 \t\n", "SELECT 1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.ToString()
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestQuery_IsEmpty(t *testing.T) {
	cases := []struct {
		name  string
		input pipelines.Query
		empty bool
	}{
		{"zero value", "", true},
		{"spaces only", "   \t \n  ", true},
		{"non empty", "SELECT 1", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.empty, tc.input.IsEmpty())
		})
	}
}

func TestQuery_Minify(t *testing.T) {
	cases := []struct {
		name     string
		input    pipelines.Query
		expected string
	}{
		{"empty", "", ""},
		{"already minimal", "SELECT 1", "SELECT 1"},
		{
			name:     "collapse spaces and newlines",
			input:    "\n  SELECT   count(*)  FROM   tbl  \n WHERE   x =  1 \n",
			expected: "SELECT count(*) FROM tbl WHERE x = 1",
		}, {
			name:     "tabs and mixed whitespace",
			input:    "\tSELECT\t*\tFROM\tusers\nWHERE\tid\t=\t42\t",
			expected: "SELECT * FROM users WHERE id = 42",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.Minify()
			assert.Equal(t, tc.expected, got)
		})
	}
}

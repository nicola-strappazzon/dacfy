package columns_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines/columns"

	"github.com/stretchr/testify/assert"
)

func TestName_ToString(t *testing.T) {
	cases := []struct {
		name     string
		input    columns.Name
		expected string
	}{
		{"empty", columns.Name(""), ""},
		{"simple", columns.Name("created_at"), "created_at"},
		{"function style", columns.Name("toYYYYMM(created_at)"), "toYYYYMM(created_at)"},
		{"whitespace", columns.Name("  "), "  "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.input.ToString())
		})
	}
}

func TestName_Clear(t *testing.T) {
	cases := []struct {
		name     string
		input    columns.Name
		expected string
	}{
		{"simple name", columns.Name("visit_date"), "visit_date"},
		{"function with column", columns.Name("toYYYYMM(visit_date)"), "visit_date"},
		{"other function", columns.Name("func_name(user_id)"), "user_id"},
		{"underscore in name", columns.Name("func_name(user_id_123)"), "user_id_123"},
		{"invalid format", columns.Name("sum(user id)"), "sum(user id)"},
		{"empty", columns.Name(""), ""},
		{"no parentheses", columns.Name("col123"), "col123"},
		{"invalid characters", columns.Name("col-123"), "col-123"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.input.Clear())
		})
	}
}

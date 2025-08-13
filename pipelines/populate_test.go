package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestPopulate_ToString(t *testing.T) {
	cases := []struct {
		name     string
		input    pipelines.Populate
		expected string
	}{
		{"zero value", "", ""},
		{"backfill", pipelines.TypeBackfill, "backfill"},
		{"chunk", pipelines.TypeChunk, "chunk"},
		{"unknown", pipelines.Populate("native"), "native"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.input.ToString())
		})
	}
}

func TestPopulate_IsEmpty_IsNotEmpty(t *testing.T) {
	cases := []struct {
		name    string
		input   pipelines.Populate
		isEmpty bool
	}{
		{"zero value", "", true},
		{"spaces only", "   \t\n  ", true}, // IsEmpty usa TrimSpace internamente
		{"backfill", pipelines.TypeBackfill, false},
		{"chunk", pipelines.TypeChunk, false},
		{"unknown", pipelines.Populate("native"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.isEmpty, tc.input.IsEmpty())
			assert.Equal(t, !tc.isEmpty, tc.input.IsNotEmpty())
		})
	}
}

func TestPopulate_IsBackFill_IsNotBackFill(t *testing.T) {
	cases := []struct {
		name       string
		input      pipelines.Populate
		isBackfill bool
	}{
		{"backfill", pipelines.TypeBackfill, true},
		{"chunk", pipelines.TypeChunk, false},
		{"zero value", "", false},
		{"unknown", pipelines.Populate("native"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.isBackfill, tc.input.IsBackFill())
			assert.Equal(t, !tc.isBackfill, tc.input.IsNotBackFill())
		})
	}
}

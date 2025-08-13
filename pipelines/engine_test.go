package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestEngine_ToString(t *testing.T) {
	cases := []struct {
		name     string
		input    pipelines.Engine
		expected string
	}{
		{"empty", "", ""},
		{"trim spaces", "   MergeTree   ", "MergeTree"},
		{"trim tabs/newlines", "\n\tAggregatingMergeTree\t\n", "AggregatingMergeTree"},
		{"replicated", "ReplicatedMergeTree", "ReplicatedMergeTree"},
		{"log family", "StripeLog", "StripeLog"},
		{"other engine", "Distributed", "Distributed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.ToString()
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestEngine_IsEmpty_And_IsNotEmpty(t *testing.T) {
	cases := []struct {
		name      string
		input     pipelines.Engine
		wantEmpty bool
	}{
		{"zero value", "", true},
		{"spaces only", "   \t\n  ", true},
		{"merge tree", "MergeTree", false},
		{"aggregating", "AggregatingMergeTree", false},
		{"summing", "SummingMergeTree", false},
		{"replacing", "ReplacingMergeTree", false},
		{"collapsing", "CollapsingMergeTree", false},
		{"versioned collapsing", "VersionedCollapsingMergeTree", false},
		{"replicated", "ReplicatedMergeTree", false},
		{"distributed", "Distributed", false},
		{"memory", "Memory", false},
		{"null", "Null", false},
		{"kafka", "Kafka", false},
		{"buffer", "Buffer", false},
		{"tiny log", "TinyLog", false},
		{"stripe log", "StripeLog", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantEmpty, tc.input.IsEmpty())
			assert.Equal(t, !tc.wantEmpty, tc.input.IsNotEmpty())
		})
	}
}

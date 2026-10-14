package pipelines_test

import (
	"math"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func number(value float64) *float64 { return &value }

func TestCheckSQL(t *testing.T) {
	cases := []struct {
		name  string
		check pipelines.Check
		sql   string
	}{
		{"not null", pipelines.Check{Field: "score", NotNull: true}, "score IS NULL"},
		{"between", pipelines.Check{Field: "score", Between: []float64{0, 100}}, "score IS NOT NULL AND NOT (score >= 0 AND score <= 100)"},
		{"greater", pipelines.Check{Field: "score", GT: number(0)}, "score IS NOT NULL AND NOT (score > 0)"},
		{"greater or equal", pipelines.Check{Field: "score", GTE: number(-1.5)}, "score IS NOT NULL AND NOT (score >= -1.5)"},
		{"less", pipelines.Check{Field: "score", LT: number(100)}, "score IS NOT NULL AND NOT (score < 100)"},
		{"less or equal", pipelines.Check{Field: "score", LTE: number(100)}, "score IS NOT NULL AND NOT (score <= 100)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := tc.check.SQL()
			require.NoError(t, err)
			assert.Equal(t, tc.sql, sql)
		})
	}
}

func TestCheckRejectsInvalidRules(t *testing.T) {
	for _, check := range []pipelines.Check{
		{Field: "score"},
		{Field: "bad field", NotNull: true},
		{Field: "score", NotNull: true, GT: number(0)},
		{Field: "score", Between: []float64{}},
		{Field: "score", Between: []float64{10}},
		{Field: "score", Between: []float64{100, 0}},
		{Field: "score", GT: number(math.NaN())},
		{Field: "score", LTE: number(math.Inf(1))},
	} {
		_, err := check.SQL()
		assert.Error(t, err)
	}
}

package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestSQL(t *testing.T) {
	test := pipelines.Test{
		Name:  "daily_revenue_examples",
		Table: "daily_revenue",
		Key:   []string{"day", "country"},
		Expected: []map[string]any{
			{"day": "2026-01-01", "country": "ES", "revenue": 25, "approved": true},
			{"day": "2026-01-02", "country": "ES", "revenue": nil, "approved": false},
		},
	}

	statement, err := test.SQL("analytics")
	require.NoError(t, err)
	assert.Equal(t, "SELECT approved, country, day, revenue FROM analytics.daily_revenue WHERE (day, country) IN (('2026-01-01', 'ES'), ('2026-01-02', 'ES'))", statement)
}

func TestTestValidate(t *testing.T) {
	valid := pipelines.Test{
		Name:  "daily_revenue_examples",
		Table: "daily_revenue",
		Key:   []string{"day"},
		Expected: []map[string]any{
			{"day": "2026-01-01", "revenue": nil},
		},
	}

	cases := []struct {
		name    string
		test    pipelines.Test
		message string
	}{
		{"valid", valid, ""},
		{"missing key", pipelines.Test{Name: "test", Table: "events", Expected: valid.Expected}, "test.key is required"},
		{"inconsistent columns", pipelines.Test{Name: "test", Table: "events", Key: []string{"id"}, Expected: []map[string]any{{"id": 1}, {"id": 2, "name": "two"}}}, "test.expected rows must define the same columns"},
		{"duplicate key", pipelines.Test{Name: "test", Table: "events", Key: []string{"id"}, Expected: []map[string]any{{"id": 1}, {"id": 1}}}, "test.expected contains duplicate key values"},
		{"equivalent key", pipelines.Test{Name: "test", Table: "events", Key: []string{"id"}, Expected: []map[string]any{{"id": 1}, {"id": "1"}}}, "test.expected contains duplicate key values"},
		{"unsupported value", pipelines.Test{Name: "test", Table: "events", Key: []string{"id"}, Expected: []map[string]any{{"id": []string{"one"}}}}, "test.expected column \"id\": unsupported value type []string"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.test.Validate()
			if tc.message == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tc.message)
		})
	}
}

func TestUniqueSQL(t *testing.T) {
	item := pipelines.Test{Name: "unique_events", Table: "events", Unique: []string{"source", "id"}, Where: "active = true"}
	sql, err := item.SQL("analytics")
	require.NoError(t, err)
	assert.Equal(t, "SELECT source, id, count() FROM analytics.events WHERE (active = true) GROUP BY source, id HAVING count() > 1 ORDER BY source, id", sql)
}

func TestChecksSQL(t *testing.T) {
	item := pipelines.Test{Name: "valid_scores", Table: "events", Where: "source = 'a'", Checks: []pipelines.Check{
		{Field: "score", NotNull: true},
		{Field: "score", GT: number(0)},
	}}
	sql, err := item.SQL("analytics")
	require.NoError(t, err)
	assert.Equal(t, "SELECT countIf(score IS NULL), countIf(score IS NOT NULL AND NOT (score > 0)) FROM analytics.events WHERE (source = 'a')", sql)
}

func TestTestRejectsMixedOrEmptyModes(t *testing.T) {
	for _, item := range []pipelines.Test{
		{Name: "test", Table: "events", Unique: []string{}},
		{Name: "test", Table: "events", Checks: []pipelines.Check{}},
		{Name: "test", Table: "events", Unique: []string{"id", "id"}},
		{Name: "test", Table: "events", Unique: []string{"invalid field"}},
		{Name: "test", Table: "events", Unique: []string{"id"}, Checks: []pipelines.Check{{Field: "id", NotNull: true}}},
		{Name: "test", Table: "events", Unique: []string{"id"}, Key: []string{"id"}},
		{Name: "test", Table: "events", Checks: []pipelines.Check{{Field: "id", NotNull: true}}, Expected: []map[string]any{{"id": 1}}},
	} {
		assert.Error(t, item.Validate())
	}
}

func TestExpectedNullKeyDoesNotCollideWithText(t *testing.T) {
	item := pipelines.Test{Name: "test", Table: "events", Key: []string{"id"}, Expected: []map[string]any{{"id": nil}, {"id": "<null>"}}}
	assert.NoError(t, item.Validate())
}

func TestTestSQLSupportsNullKey(t *testing.T) {
	test := pipelines.Test{
		Name:  "events_without_source",
		Table: "events",
		Key:   []string{"source_id"},
		Expected: []map[string]any{
			{"source_id": nil, "status": "unknown"},
		},
	}

	statement, err := test.SQL("analytics")
	require.NoError(t, err)
	assert.Contains(t, statement, "FROM analytics.events WHERE ((source_id IS NULL))")
}

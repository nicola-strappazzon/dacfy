package pipelines_test

import (
	"fmt"
	"testing"

	"github.com/goccy/go-yaml"
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

func TestFinalSQLFromYAML(t *testing.T) {
	modes := []struct {
		name string
		yaml string
		sql  string
	}{
		{"unique", "unique: [id]\n", "SELECT id, count() FROM analytics.events_v7%s WHERE (active = true) GROUP BY id HAVING count() > 1 ORDER BY id"},
		{"expected", "key: [id]\nexpected: [{id: 1}]\n", "SELECT id FROM analytics.events_v7%s WHERE (active = true) AND (id IN (1))"},
		{"checks", "checks: [{field: id, not_null: true}]\n", "SELECT countIf(id IS NULL) FROM analytics.events_v7%s WHERE (active = true)"},
	}
	for _, mode := range modes {
		for _, option := range []struct {
			name  string
			yaml  string
			final string
		}{
			{"omitted", "", ""},
			{"false", "final: false\n", ""},
			{"true", "final: true\n", " FINAL"},
		} {
			t.Run(mode.name+"/"+option.name, func(t *testing.T) {
				var item pipelines.Test
				require.NoError(t, yaml.Unmarshal([]byte("name: events\ntable: events\nwhere: active = true\n"+mode.yaml+option.yaml), &item))
				sql, err := item.SetSuffix("_v7").SQL("analytics")
				require.NoError(t, err)
				assert.Equal(t, fmt.Sprintf(mode.sql, option.final), sql)
			})
		}
	}
}

func TestFinalRejectsInvalidYAML(t *testing.T) {
	for _, value := range []string{"sometimes", "[true]", "{enabled: true}"} {
		var item pipelines.Test
		assert.Error(t, yaml.Unmarshal([]byte("final: "+value), &item))
	}
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

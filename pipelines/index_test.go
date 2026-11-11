package pipelines_test

import (
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexSQLFromYAML(t *testing.T) {
	var table pipelines.Table
	require.NoError(t, yaml.Unmarshal([]byte(`name: events
indexes:
  - { name: idx_search_file, expression: search_file, type: "text(tokenizer = ngrams(3))", granularity: 1 }
  - { name: idx_sha256_bf, expression: sha256, type: bloom_filter(0.01) }
`), &table))
	table.Parent = &pipelines.Pipelines{Database: pipelines.Database{Name: "analytics"}}

	assert.Equal(t, []string{
		"ALTER TABLE analytics.events_v2 ADD INDEX IF NOT EXISTS idx_search_file search_file TYPE text(tokenizer = ngrams(3)) GRANULARITY 1",
		"ALTER TABLE analytics.events_v2 ADD INDEX IF NOT EXISTS idx_sha256_bf sha256 TYPE bloom_filter(0.01)",
	}, table.SetSuffix("_v2").AddIndexes())
}

func TestIndexValidate(t *testing.T) {
	valid := pipelines.Index{Name: "idx_sha256", Expression: "sha256", Type: "bloom_filter(0.01)"}
	cases := []struct {
		name    string
		indexes []pipelines.Index
		message string
	}{
		{"valid", []pipelines.Index{valid}, ""},
		{"invalid name", []pipelines.Index{{Name: "1idx", Expression: "sha256", Type: "minmax"}}, `table "events": index.name "1idx" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`},
		{"missing expression", []pipelines.Index{{Name: "idx", Type: "minmax"}}, `table "events": index.expression is required for index "idx"`},
		{"missing type", []pipelines.Index{{Name: "idx", Expression: "sha256"}}, `table "events": index.type is required for index "idx"`},
		{"repeated name", []pipelines.Index{valid, valid}, `table "events": index.name "idx_sha256" is repeated`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var table pipelines.Table
			require.NoError(t, yaml.Unmarshal([]byte("name: events\nengine: MergeTree\ncolumns: [{name: sha256, type: String}]\n"), &table))
			table.Indexes = tc.indexes
			err := table.Validate()
			if tc.message == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tc.message)
		})
	}
}

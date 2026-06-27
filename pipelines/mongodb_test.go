package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestMongoDB_IsEmpty(t *testing.T) {
	cases := []struct {
		name     string
		input    pipelines.MongoDB
		expected bool
	}{
		{"empty", pipelines.MongoDB{}, true},
		{"only uri", pipelines.MongoDB{URI: "mongodb://localhost:27017/db"}, false},
		{"full", pipelines.MongoDB{URI: "mongodb://localhost:27017/db", Collection: "col"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.input.IsEmpty())
		})
	}
}

func TestMongoDB_Validate(t *testing.T) {
	cases := []struct {
		name    string
		input   pipelines.MongoDB
		wantErr string
	}{
		{"missing uri", pipelines.MongoDB{}, "mongodb.uri is required"},
		{"missing collection", pipelines.MongoDB{URI: "mongodb://localhost:27017/db"}, "mongodb.collection is required"},
		{"valid simple", pipelines.MongoDB{URI: "mongodb://localhost:27017/db", Collection: "col"}, ""},
		{"valid atlas", pipelines.MongoDB{
			URI:        "mongodb://user:pass@host1:27017,host2:27017/db?ssl=true&replicaSet=rs0&authSource=admin",
			Collection: "records",
		}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.input.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.wantErr)
			}
		})
	}
}

func TestMongoDB_EngineString(t *testing.T) {
	m := pipelines.MongoDB{
		URI:        "mongodb://user:pass@host1:27017,host2:27017/mydb?ssl=true&replicaSet=rs0",
		Collection: "records",
	}

	expected := "MongoDB('mongodb://user:pass@host1:27017,host2:27017/mydb?ssl=true&replicaSet=rs0', 'records')"
	assert.Equal(t, expected, m.EngineString())
}

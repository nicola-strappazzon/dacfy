package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyFormat(t *testing.T) {
	assert.Equal(t, "id=101, region=eu", key{"id", "region"}.Format(map[string]any{
		"id":     101,
		"region": "eu",
	}))
}

package env

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	t.Setenv("TEST_STRING", "value")

	assert.Equal(t, "value", String("TEST_STRING", "fallback"))
	assert.Equal(t, "fallback", String("MISSING_STRING", "fallback"))
}

func TestBool(t *testing.T) {
	t.Setenv("TEST_BOOL", "true")

	value, err := Bool("TEST_BOOL", false)
	require.NoError(t, err)
	assert.True(t, value)

	value, err = Bool("MISSING_BOOL", true)
	require.NoError(t, err)
	assert.True(t, value)
}

func TestBoolRejectsInvalidValue(t *testing.T) {
	t.Setenv("TEST_BOOL", "sometimes")

	_, err := Bool("TEST_BOOL", false)
	assert.EqualError(t, err, "TEST_BOOL must be a boolean: strconv.ParseBool: parsing \"sometimes\": invalid syntax")
}

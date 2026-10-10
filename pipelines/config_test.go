package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Load(t *testing.T) {
	t.Setenv("DACFY_HOST", "clickhouse.example.com:9440")
	t.Setenv("DACFY_USER", "analytics")
	t.Setenv("DACFY_PASSWORD", "secret")
	t.Setenv("DACFY_TLS", "true")
	t.Setenv("DACFY_TLS_INSECURE", "true")

	var config pipelines.Config
	require.NoError(t, config.Load())

	assert.Equal(t, "clickhouse.example.com:9440", config.Host)
	assert.Equal(t, "analytics", config.User)
	assert.Equal(t, "secret", config.Password)
	assert.True(t, config.TLS)
	assert.True(t, config.TLSInsecure)
}

func TestConfig_LoadRejectsInvalidBoolean(t *testing.T) {
	t.Setenv("DACFY_TLS", "sometimes")

	var config pipelines.Config
	assert.EqualError(t, config.Load(), "DACFY_TLS must be a boolean: strconv.ParseBool: parsing \"sometimes\": invalid syntax")
}

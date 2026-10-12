package ping_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/ping"
	"github.com/stretchr/testify/assert"
)

func TestNewCommand(t *testing.T) {
	cmd := ping.NewCommand()

	assert.Equal(t, "ping", cmd.Use)
	assert.Equal(t, "Verify the ClickHouse connection.", cmd.Short)
	assert.NoError(t, cmd.Args(cmd, nil))
	assert.Error(t, cmd.Args(cmd, []string{"unexpected"}))
}

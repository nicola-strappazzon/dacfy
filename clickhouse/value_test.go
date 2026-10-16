package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueText(t *testing.T) {
	assert.Equal(t, "<null>", NewValue(nil).Text())
	assert.Equal(t, "hello", NewValue("hello").Text())
}

func TestValueEqual(t *testing.T) {
	assert.True(t, NewValue(101).Equal(NewValue(uint64(101))))
	assert.False(t, NewValue(101).Equal(NewValue(102)))
	assert.True(t, NewValue(nil).Equal(NewValue(nil)))
	assert.False(t, NewValue(nil).Equal(NewValue("<null>")))
	assert.False(t, NewValue("<null>").Equal(NewValue(nil)))
}

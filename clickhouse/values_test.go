package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValuesEqual(t *testing.T) {
	expected := Values{NewValue(101), NewValue("active")}

	assert.True(t, expected.Equal(Values{NewValue(uint64(101)), NewValue("active")}))
	assert.False(t, expected.Equal(Values{NewValue(101)}))
}

func TestValuesKey(t *testing.T) {
	values := Values{NewValue(101), NewValue("eu,west")}

	assert.Equal(t, `"101","eu,west"`, values.Key([]int{0, 1}))
	assert.NotEqual(t, Values{NewValue(nil)}.Key([]int{0}), Values{NewValue("<null>")}.Key([]int{0}))
}

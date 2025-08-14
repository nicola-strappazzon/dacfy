package strings_test

import (
	stdstrings "strings"
	"testing"

	mystrings "github.com/nicola-strappazzon/dacfy/strings"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_ZeroValue_WriteAndString(t *testing.T) {
	var b mystrings.Builder
	n, err := b.WriteString("hello")
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", b.String())

	n, err = b.WriteString("")
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, "hello", b.String())
}

func TestBuilder_MultipleWrites_CountsAndContent(t *testing.T) {
	var b mystrings.Builder
	total := 0

	n, err := b.WriteString("foo")
	assert.NoError(t, err)
	total += n

	n, err = b.WriteString("bar")
	assert.NoError(t, err)
	total += n

	assert.Equal(t, 6, total)
	assert.Equal(t, "foobar", b.String())

	var sb stdstrings.Builder
	sb.WriteString("foo")
	sb.WriteString("bar")
	assert.Equal(t, sb.String(), b.String())
}

func TestBuilder_NilPointer_PanicsOnUse(t *testing.T) {
	var b *mystrings.Builder = nil
	assert.Panics(t, func() {
		_, _ = b.WriteString("boom")
	})
	assert.Panics(t, func() {
		_ = b.String()
	})
}

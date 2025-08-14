package pipelines_test

import (
	"strings"
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestName_ToString(t *testing.T) {
	cases := []struct {
		name   string
		input  pipelines.Name
		expect string
	}{
		{"empty", pipelines.Name(""), ""},
		{"database name", pipelines.Name("analytics"), "analytics"},
		{"table/column with underscore", pipelines.Name("events_2025"), "events_2025"},
		{"raw whitespace kept", pipelines.Name("  "), "  "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, tc.input.ToString())
		})
	}
}

func TestName_IsEmpty_And_IsNotEmpty(t *testing.T) {
	var zero pipelines.Name

	cases := []struct {
		name  string
		input pipelines.Name
		empty bool
	}{
		{"zero value", zero, true},
		{"empty literal", pipelines.Name(""), true},
		{"only spaces -> empty", pipelines.Name("   "), true},
		{"db name", pipelines.Name("db01"), false},
		{"column name", pipelines.Name("user_id"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.empty, tc.input.IsEmpty())
			assert.Equal(t, !tc.empty, tc.input.IsNotEmpty())
		})
	}
}

func TestName_IsNotValid(t *testing.T) {
	t.Run("valid minimal length (2)", func(t *testing.T) {
		n := pipelines.Name("db")
		assert.False(t, n.IsNotValid())
	})

	t.Run("valid with underscore and digits", func(t *testing.T) {
		n := pipelines.Name("events_2025")
		assert.False(t, n.IsNotValid())
	})

	t.Run("valid max length 255", func(t *testing.T) {
		// ^[A-Za-z][A-Za-z0-9_]{1,254}$  => total length 2..255
		n := pipelines.Name("d" + strings.Repeat("a", 254)) // len=255
		assert.False(t, n.IsNotValid())
	})

	t.Run("invalid length 256", func(t *testing.T) {
		n := pipelines.Name("d" + strings.Repeat("a", 255)) // len=256
		assert.True(t, n.IsNotValid())
	})

	t.Run("invalid: starts with digit", func(t *testing.T) {
		n := pipelines.Name("1analytics")
		assert.True(t, n.IsNotValid())
	})

	t.Run("invalid: dash not allowed", func(t *testing.T) {
		n := pipelines.Name("user-events")
		assert.True(t, n.IsNotValid())
	})

	t.Run("invalid: single char (needs >=2)", func(t *testing.T) {
		n := pipelines.Name("d")
		assert.True(t, n.IsNotValid())
	})

	t.Run("invalid: non-ascii letter (ñ) not allowed by regex", func(t *testing.T) {
		n := pipelines.Name("maña")
		assert.True(t, n.IsNotValid())
	})

	t.Run("empty is invalid (and empty)", func(t *testing.T) {
		n := pipelines.Name("")
		assert.True(t, n.IsNotValid())
		assert.True(t, n.IsEmpty())
	})
}

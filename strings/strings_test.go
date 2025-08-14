package strings_test

import (
	stdstrings "strings"
	"testing"

	mystrings "github.com/nicola-strappazzon/dacfy/strings"

	"github.com/stretchr/testify/assert"
)

func TestJoin(t *testing.T) {
	t.Run("nil slice", func(t *testing.T) {
		var in []string
		assert.Equal(t, "", mystrings.Join(in))
	})

	t.Run("empty slice", func(t *testing.T) {
		in := []string{}
		assert.Equal(t, "", mystrings.Join(in))
	})

	t.Run("single element", func(t *testing.T) {
		in := []string{"a"}
		assert.Equal(t, "a", mystrings.Join(in))
	})

	t.Run("multiple elements with comma no space", func(t *testing.T) {
		in := []string{"a", "b", "c"}
		assert.Equal(t, "a,b,c", mystrings.Join(in)) // tu Join usa "," sin espacio
	})

	t.Run("elements with spaces preserved", func(t *testing.T) {
		in := []string{" a ", "b", " c"}
		assert.Equal(t, " a ,b, c", mystrings.Join(in))
	})

	t.Run("match stdlib behavior", func(t *testing.T) {
		in := []string{"x", "y"}
		assert.Equal(t, stdstrings.Join(in, ","), mystrings.Join(in))
	})
}

func TestTrimSpace(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		expect string
	}{
		{"empty", "", ""},
		{"spaces", "   hi   ", "hi"},
		{"tabs/newlines", "\n\t hi \t\n", "hi"},
		{"no trim needed", "hi", "hi"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, mystrings.TrimSpace(tc.in))
		})
	}
}

func TestIsEmpty_IsNotEmpty(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		expect bool
	}{
		{"empty", "", true},
		{"spaces only", "   \t\n  ", true},
		{"non-empty", "hi", false},
		{"non-empty with spaces", "  hi  ", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, mystrings.IsEmpty(tc.in))
			assert.Equal(t, !tc.expect, mystrings.IsNotEmpty(tc.in))
		})
	}
}

func TestContains(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		out    string
		expect bool
	}{
		{"found", "hello world", "world", true},
		{"not found", "hello", "xyz", false},
		{"empty substr -> true", "anything", "", true},
		{"both empty -> true", "", "", true},
		{"unicode", "mañana", "ña", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, mystrings.Contains(tc.in, tc.out))
		})
	}
}

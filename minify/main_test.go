package minify_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/minify"

	"github.com/stretchr/testify/assert"
)

func TestMinify(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		expected string
	}{
		{
			name:     "trim trailing space",
			in:       "SELECT 1   ",
			expected: "SELECT 1",
		}, {
			name:     "collapse whitespace",
			in:       "\n  SELECT   a,\t b  \nFROM   t  \t WHERE   x =  1 \n",
			expected: "SELECT a, b FROM t WHERE x = 1",
		}, {
			name:     "tabs and newlines",
			in:       "\tSELECT\t*\nFROM\t`t`\nWHERE\tid\t=\t42\t",
			expected: "SELECT * FROM t WHERE id = 42",
		}, {
			name:     "single-line comment at end of line",
			in:       "SELECT 1 -- comment\nFROM t",
			expected: "SELECT 1 FROM t",
		}, {
			name:     "whole line comment is removed",
			in:       "-- header\nSELECT 1",
			expected: "SELECT 1",
		}, {
			name:     "single-line comment with CRLF",
			in:       "SELECT 1 -- comment\r\nFROM t",
			expected: "SELECT 1 FROM t",
		}, {
			name:     "inline block comment",
			in:       "SELECT /* comment */ 1",
			expected: "SELECT 1",
		}, {
			name:     "spanning block comment",
			in:       "SELECT /* a \n b \n c */ 1",
			expected: "SELECT 1",
		}, {
			name:     "remove backticks around identifiers",
			in:       "SELECT `col`, `from` FROM `db`.`t`",
			expected: "SELECT col, from FROM db.t",
		}, {
			name:     "multiple spaces become single",
			in:       "a    b",
			expected: "a b",
		}, {
			name:     "no trailing space at end",
			in:       "SELECT a FROM t    ",
			expected: "SELECT a FROM t",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := minify.Minify(tc.in)
			assert.Equal(t, tc.expected, got)
		})
	}
}

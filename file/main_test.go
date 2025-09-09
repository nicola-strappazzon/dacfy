package file_test

import (
	"os"
	"testing"

	"github.com/nicola-strappazzon/dacfy/file"

	"github.com/stretchr/testify/assert"
)

func TestReadExpandEnv(t *testing.T) {
	os.Setenv("FOO", "bar")
	defer os.Unsetenv("FOO")

	cases := []struct {
		name string
		in   []byte
		want []byte
	}{
		{
			name: "Exist env var",
			in:   []byte("valor=$FOO"),
			want: []byte("valor=bar"),
		},
		{
			name: "Unexisted env var",
			in:   []byte("value=$BAZ"),
			want: []byte("value="),
		},
		{
			name: "String without env var",
			in:   []byte("plain text"),
			want: []byte("plain text"),
		},
		{
			name: "Empty string",
			in:   []byte(""),
			want: []byte(""),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := file.ReadExpandEnv(c.in)
			assert.Equal(t, string(c.want), string(got))
		})
	}
}

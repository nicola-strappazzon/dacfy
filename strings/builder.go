package strings

import (
	"strings"
)

type Builder strings.Builder

func (b *Builder) WriteString(in string) (int, error) {
	return (*strings.Builder)(b).WriteString(in)
}

func (b *Builder) String() string {
	return (*strings.Builder)(b).String()
}

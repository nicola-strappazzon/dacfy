package columns

import (
	"fmt"

	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Map []struct {
	Name string `yaml:"name"` // Column name.
	Type string `yaml:"type"` // Column data type or function.
}

func (m Map) JoinWithTypes() string {
	var i []string
	for _, c := range m {
		i = append(i, fmt.Sprintf("%s %s", c.Name, c.Type))
	}
	return strings.Join(i)
}

func (m Map) JoinWithoutTypes() string {
	var i []string
	for _, c := range m {
		i = append(i, c.Name)
	}
	return strings.Join(i)
}

func (m Map) IsEmpty() bool {
	return len(m) == 0
}

func (m Map) IsNotEmpty() bool {
	return !m.IsEmpty()
}

func (m Map) ToArray() (out Array) {
	for _, c := range m {
		out = append(out, Name(c.Name))
	}

	return out
}

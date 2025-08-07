package pipelines

import (
	"fmt"
	"regexp"

	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type ColumnArray string

type ColumnMap struct {
	Name string `yaml:"name"` // Column name.
	Type string `yaml:"type"` // Column data type.
	View bool   `yaml:"view"` // Enable this when the populate process requires a query-based method.
}

type ColumnsMap []ColumnMap
type ColumnsArray []ColumnArray

func (c ColumnsMap) WithTypes() string {
	var columns []string
	for _, column := range c {
		columns = append(columns, fmt.Sprintf("%s %s", column.Name, column.Type))
	}
	return strings.Join(columns)
}

func (c ColumnsMap) WithoutTypes() string {
	var columns []string
	for _, column := range c {
		columns = append(columns, column.Name)
	}
	return strings.Join(columns)
}

func (c ColumnArray) ToString() string {
	return string(c)
}

func (c ColumnArray) Clear() string {
	re := regexp.MustCompile(`^(?:\w+\()?([a-zA-Z_][a-zA-Z0-9_]*)\)?$`)

	match := re.FindStringSubmatch(c.ToString())
	if len(match) > 1 {
		return match[1]
	}

	return c.ToString()
}

func (c ColumnsArray) ToArrayString() []string {
	r := make([]string, len(c))
	for i, val := range c {
		r[i] = val.ToString()
	}
	return r
}

func (c ColumnsArray) Join() string {
	return strings.Join(c.ToArrayString())
}

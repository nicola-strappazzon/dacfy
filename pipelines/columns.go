package pipelines

import (
	"fmt"

	"github.com/nicola-strappazzon/clickhouse-dac/strings"
)

type Column struct {
	Name string `yaml:"name"` // Column name.
	Type string `yaml:"type"` // Column data type.
	View bool   `yaml:"view"` // Enable this when the populate process requires a query-based method.
}

type Columns []Column

func (c Columns) WithTypes() string {
	var columns []string
	for _, column := range c {
		columns = append(columns, fmt.Sprintf("%s %s", column.Name, column.Type))
	}
	return strings.Join(columns)
}

func (c Columns) WithoutTypes() string {
	var columns []string
	for _, column := range c {
		columns = append(columns, column.Name)
	}
	return strings.Join(columns)
}

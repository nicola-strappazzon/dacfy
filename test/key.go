package test

import (
	"strings"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
)

type key []string

func (k key) Format(row map[string]any) string {
	values := make(clickhouse.Values, len(k))
	for i, name := range k {
		values[i] = clickhouse.NewValue(row[name])
	}
	return k.FormatValues(values)
}

func (k key) FormatValues(row clickhouse.Values) string {
	values := make([]string, len(k))
	for i, name := range k {
		values[i] = name + "=" + row[i].Text()
	}
	return strings.Join(values, ", ")
}

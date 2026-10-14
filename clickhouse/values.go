package clickhouse

import (
	"strings"
)

type Values []Value

func (v Values) Equal(other Values) bool {
	if len(v) != len(other) {
		return false
	}
	for i := range v {
		if !v[i].Equal(other[i]) {
			return false
		}
	}
	return true
}

func (v Values) Key(indexes []int) string {
	values := make([]string, len(indexes))
	for i, index := range indexes {
		values[i] = v[index].Key()
	}
	return strings.Join(values, ",")
}

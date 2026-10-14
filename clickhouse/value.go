package clickhouse

import (
	"fmt"
	"strconv"
)

type Value struct {
	raw any
}

func NewValue(raw any) Value {
	return Value{raw: raw}
}

func (v Value) Equal(other Value) bool {
	if v.raw == nil || other.raw == nil {
		return v.raw == nil && other.raw == nil
	}
	return v.Text() == other.Text()
}

func (v Value) Key() string {
	if v.raw == nil {
		return "NULL"
	}
	return strconv.Quote(v.Text())
}

func (v Value) Text() string {
	if v.raw == nil {
		return "<null>"
	}
	return fmt.Sprint(v.raw)
}

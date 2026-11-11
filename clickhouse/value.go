package clickhouse

import (
	"fmt"
	"reflect"
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
	if v.isList() || other.isList() {
		return v.isList() && other.isList() && v.equalList(other)
	}
	return v.Text() == other.Text()
}

func (v Value) isList() bool {
	kind := reflect.ValueOf(v.raw).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// equalList compares lists item by item, so ["a b", "c"] and ["a", "b c"]
// are different even though both print as [a b c].
func (v Value) equalList(other Value) bool {
	left, right := reflect.ValueOf(v.raw), reflect.ValueOf(other.raw)
	if left.Len() != right.Len() {
		return false
	}
	for i := range left.Len() {
		item := NewValue(scannedValue(left.Index(i).Interface()))
		if !item.Equal(NewValue(scannedValue(right.Index(i).Interface()))) {
			return false
		}
	}
	return true
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

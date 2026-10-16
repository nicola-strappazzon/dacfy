package clickhouse

import (
	"reflect"
)

type Result struct {
	Columns []Column
	Rows    []Values
}

func (r *Result) Read(rows rows) error {
	*r = Result{}
	types := rows.ColumnTypes()
	r.Columns = make([]Column, len(types))
	for i, item := range types {
		r.Columns[i] = Column{Name: item.Name()}
	}

	for rows.Next() {
		values := make(Values, len(types))
		destinations := make([]any, len(types))
		for i, item := range types {
			destinations[i] = reflect.New(item.ScanType()).Interface()
		}
		if err := rows.Scan(destinations...); err != nil {
			return err
		}
		for i, destination := range destinations {
			values[i] = NewValue(scannedValue(destination))
		}
		r.Rows = append(r.Rows, values)
	}

	return rows.Err()
}

func scannedValue(destination any) any {
	value := reflect.ValueOf(destination)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}

	return value.Interface()
}

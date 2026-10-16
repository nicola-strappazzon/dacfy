package clickhouse

import (
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
)

type resultColumnType struct{}

func (resultColumnType) Name() string             { return "name" }
func (resultColumnType) Nullable() bool           { return false }
func (resultColumnType) ScanType() reflect.Type   { return reflect.TypeFor[string]() }
func (resultColumnType) DatabaseTypeName() string { return "String" }

type resultRows struct {
	next bool
}

func (r *resultRows) Next() bool {
	if !r.next {
		return false
	}
	r.next = false
	return true
}

func (resultRows) Scan(dest ...any) error {
	*dest[0].(*string) = "dacfy"
	return nil
}

func (resultRows) ColumnTypes() []driver.ColumnType { return []driver.ColumnType{resultColumnType{}} }
func (resultRows) Err() error                       { return nil }

func TestResultRead(t *testing.T) {
	var result Result

	err := result.Read(&resultRows{next: true})

	assert.NoError(t, err)
	assert.Equal(t, []Column{{Name: "name"}}, result.Columns)
	assert.Equal(t, []Values{{NewValue("dacfy")}}, result.Rows)

	// Reusing a result replaces the previous rows, including for an empty query.
	assert.NoError(t, result.Read(&resultRows{}))
	assert.Empty(t, result.Rows)
}

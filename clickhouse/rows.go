package clickhouse

import "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

type rows interface {
	Next() bool
	Scan(...any) error
	ColumnTypes() []driver.ColumnType
	Err() error
}

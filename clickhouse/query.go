package clickhouse

import (
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Settings are ClickHouse settings sent with one query.
type Settings = clickhouse.Settings

// Query runs a statement with the given settings and reads all rows.
func (ch *ClickHouse) Query(statement string, settings Settings) (Result, error) {
	if ch.IsNotConnected() {
		return Result{}, errors.New("connection to ClickHouse server not established, you may be using the --dry-run flag")
	}

	ctx := clickhouse.Context(ch.Context, clickhouse.WithSettings(settings))
	rows, err := ch.Connection.Query(ctx, statement)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()

	var result Result
	if err := result.Read(rows); err != nil {
		return Result{}, err
	}
	return result, nil
}

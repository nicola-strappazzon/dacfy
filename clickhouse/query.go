package clickhouse

import (
	"errors"
)

func (ch *ClickHouse) Query(statement string) (Result, error) {
	if ch.IsNotConnected() {
		return Result{}, errors.New("connection to ClickHouse server not established, you may be using the --dry-run flag")
	}

	rows, err := ch.Connection.Query(ch.Context, statement)
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

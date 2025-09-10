package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/version"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

var instance *ClickHouse

func Instance() *ClickHouse {
	if instance == nil {
		instance = &ClickHouse{}
	}

	return instance
}

type ClickHouse struct {
	Connection driver.Conn
	QueryID    string
	Context    context.Context
}

func (ch *ClickHouse) NewQueryID() string {
	ch.QueryID = uuid.NewString()
	return ch.QueryID
}

func (ch *ClickHouse) Connect() (err error) {
	var pl = pipelines.Instance()

	options := &clickhouse.Options{
		Addr:     []string{pl.Config.Host},
		Protocol: clickhouse.Native,
		Debug:    false,
		Auth: clickhouse.Auth{
			Username: pl.Config.User,
			Password: pl.Config.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 0,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		ReadTimeout: time.Second * 28800,
		DialTimeout: time.Second * 30,
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "cht", Version: version.VERSION},
			},
		},
	}

	if pl.Config.TLS {
		options.TLS = &tls.Config{}
	}

	ch.Connection, err = clickhouse.Open(options)
	if err != nil {
		return err
	}

	ch.Context = clickhouse.Context(context.Background())

	return ch.Connection.Ping(ch.Context)
}

func (ch *ClickHouse) Execute(in string, logger bool) (err error) {
	if logger {
		err = ch.ExecuteWitchLogger(in)
	} else {
		err = ch.ExecuteWitchOutLogger(in)
	}

	return err
}

func (ch *ClickHouse) ExecuteWitchOutLogger(in string) error {
	if err := ch.Connection.Exec(ch.Context, in); err != nil {
		return err
	}

	return nil
}

func (ch *ClickHouse) ExecuteWitchLogger(in string) error {
	// ch.Progress.StartNow()
	ch.NewQueryID()

	ctx := clickhouse.Context(context.Background(),
		clickhouse.WithQueryID(ch.QueryID),
		// clickhouse.WithProgress(func(p *clickhouse.Progress) {
		// 	ch.Progress.SetReadRows(p.Rows)
		// 	ch.Progress.SetReadBytes(p.Bytes)
		// 	ch.Progress.SetTotalRows(p.TotalRows)

		// 	// go ch.WriteProcess()
		// }),
	)

	if err := ch.Connection.Exec(ctx, in); err != nil {
		return err
	}

	// ch.WriteProcess()

	return nil
}

func (ch *ClickHouse) DatabaseExists(in string) (out bool) {
	if ch.IsNotConnected() {
		return out
	}

	ch.Connection.QueryRow(
		ch.Context,
		fmt.Sprintf("SELECT true FROM system.databases WHERE name = '%s';", in),
	).Scan(&out)

	return out
}

func (ch *ClickHouse) IsNotConnected() bool {
	return ch.Connection == nil
}

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
	Progress   Progress
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

	return ch.Connection.Ping(context.Background())
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
	ctx := clickhouse.Context(context.Background())

	if err := ch.Connection.Exec(ctx, in); err != nil {
		return err
	}

	return nil
}

func (ch *ClickHouse) ExecuteWitchLogger(in string) error {
	ch.Progress.StartNow()
	ch.NewQueryID()

	ctx := clickhouse.Context(context.Background(),
		clickhouse.WithQueryID(ch.QueryID),
		clickhouse.WithProgress(func(p *clickhouse.Progress) {
			ch.Progress.SetReadRows(p.Rows)
			ch.Progress.SetReadBytes(p.Bytes)
			ch.Progress.SetTotalRows(p.TotalRows)

			go func() {
				ch.GatherSystemProcess()
				loggerProgress.WriteProgress(ch.Progress)
			}()
		}),
	)

	if err := ch.Connection.Exec(ctx, in); err != nil {
		return err
	}

	return nil
}

func (ch *ClickHouse) GatherSystemProcess() error {
	sql := fmt.Sprintf("SELECT toUInt64(memory_usage) AS memory, toUInt64(peak_memory_usage) AS PeakMemory, ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 100000 AS cpu FROM system.processes WHERE query_id = '%s'", ch.QueryID)
	ctx := clickhouse.Context(context.Background())

	if err := ch.Connection.QueryRow(ctx, sql).Scan(&ch.Progress.Memory, &ch.Progress.PeakMemory, &ch.Progress.CPU); err != nil {
		return err
	}

	return nil
}

func (ch *ClickHouse) SetLogger(in LoggerProgress) {
	loggerProgress = in
}

package create

import (
	"fmt"

	"github.com/nicola-strappazzon/clickhouse-dac/clickhouse"
	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"
	"github.com/nicola-strappazzon/clickhouse-dac/strings"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "create",
		Short:   "Create tables and materialized views as defined in the pipelines.",
		Example: `clickhouse-dac create --pipe=foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	cmd.MarkFlagRequired("pipe")

	return cmd
}

func Run() (err error) {
	queries := []struct {
		Message   string
		Statement string
	}{
		{
			Statement: pl.Database.Create().DML(),
			Message:   fmt.Sprintf("Create database: %s", pl.Database.Name),
		},
		{
			Statement: pl.Database.Use().DML(),
		},
		{
			Statement: pl.Table.Create().DML(),
			Message:   fmt.Sprintf("Create table: %s", pl.Table.Name),
		},
		{
			Statement: pl.Table.Query.ToString(),
		},
		{
			Statement: pl.View.Create().DML(),
			Message:   fmt.Sprintf("Create view: %s", pl.View.Name),
		},
	}

	for _, query := range queries {
		if strings.IsEmpty(query.Statement) {
			continue
		}

		if strings.IsNotEmpty(query.Message) {
			fmt.Println("-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Println(query.Statement)
		}

		if err = ch.Execute(query.Statement); err != nil {
			return err
		}
	}

	return nil
}

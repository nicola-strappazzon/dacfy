package deploy

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
		Use:     "deploy",
		Short:   "Create tables and materialized views, and populate data as defined in the pipelines.",
		Long:    ``,
		Example: `dac deploy --host=demo.clickhouse.cloud --user=default --password=mypass --pipe=foo.yaml`,
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
		Statement string
		Logger    bool
	}{
		{Statement: pl.Database.Create().DML()},
		{Statement: pl.Database.Use().DML()},
		{Statement: pl.Table.Create().DML()},
		{Statement: pl.Table.Query.String()},
		{Statement: pl.View.Create().DML()},
		{Statement: pl.Populate().DML(), Logger: true},
	}

	for _, query := range queries {
		if strings.IsEmpty(query.Statement) {
			continue
		}

		fmt.Println("==> Query:", query.Statement)

		if query.Logger {
			err = ch.ExecuteWitchLogger(query.Statement)
		} else {
			err = ch.Execute(query.Statement)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

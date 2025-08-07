package destroy

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
		Use:     "destroy",
		Short:   "Remove tables and materialized views as defined in the pipelines.",
		Example: `clickhouse-dac destroy --pipe=foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	cmd.MarkFlagRequired("pipe")

	return cmd
}

func Run() error {
	queries := []struct {
		Message   string
		Statement string
		Delete    bool
	}{
		{
			Statement: pl.View.Drop().DML(),
			Delete:    pl.View.Delete,
			Message:   fmt.Sprintf("Delete view: %s", pl.View.Name),
		},
		{
			Statement: pl.Table.Drop().DML(),
			Delete:    pl.Table.Delete,
			Message:   fmt.Sprintf("Delete table: %s", pl.Table.Name),
		},
		{
			Statement: pl.Database.Drop().DML(),
			Delete:    pl.Database.Delete,
			Message:   fmt.Sprintf("Delete database: %s", pl.Database.Name),
		},
	}

	for _, query := range queries {
		if query.Delete == false {
			continue
		}

		if strings.IsEmpty(query.Statement) {
			continue
		}

		if strings.IsNotEmpty(query.Message) {
			fmt.Println("-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Println(query.Statement)
		}

		if err := ch.Execute(query.Statement); err != nil {
			return err
		}
	}

	return nil
}

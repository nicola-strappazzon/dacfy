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
		Long:    ``,
		Example: `dac destroy --host=demo.clickhouse.cloud --user=default --password=mypass --pipe=foo.yaml`,
		Run: func(cmd *cobra.Command, args []string) {
			Run()
		},
	}

	cmd.Flags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	cmd.MarkFlagRequired("pipe")

	return cmd
}

func Run() {
	var err error

	queries := []struct {
		Statement string
		Delete    bool
	}{
		{Statement: pl.View.Drop().DML(), Delete: pl.View.Delete},
		{Statement: pl.Table.Drop().DML(), Delete: pl.Table.Delete},
		{Statement: pl.Database.Drop().DML(), Delete: pl.Database.Delete},
	}

	for _, query := range queries {
		if !query.Delete {
			continue
		}

		if strings.IsEmpty(query.Statement) {
			continue
		}

		fmt.Println("==> Query:", query.Statement)

		if err = ch.Execute(query.Statement); err != nil {
			panic(err)
		}
	}
}

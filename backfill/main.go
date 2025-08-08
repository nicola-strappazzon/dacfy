package backfill

import (
	"fmt"

	"github.com/nicola-strappazzon/clickhouse-dac/clickhouse"
	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "backfill",
		Short:   "Backfill tables as defined in the pipelines.",
		Example: `clickhouse-dac backfill --pipe=foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	cmd.MarkFlagRequired("pipe")

	return cmd
}

func Run() error {
	fmt.Println("--> Starting to backfill the table:", pl.PopulateTableName())

	if pl.Config.SQL {
		fmt.Println(pl.Backfill().DML())
	}

	err := ch.ExecuteWitchLogger(pl.Backfill().DML())
	fmt.Println("")
	return err
}

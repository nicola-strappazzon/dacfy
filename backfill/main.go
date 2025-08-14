package backfill

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "backfill",
		Short:   "Backfill tables as defined in the pipelines.",
		Example: `dacfy backfill --pipe=foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	cmd.MarkFlagRequired("pipe")

	return cmd
}

func Run() (err error) {
	if err = pl.Backfill.Validate(); err != nil {
		return err
	}

	fmt.Println("--> Starting to backfill the table:", pl.View.To)

	if pl.Config.SQL {
		fmt.Println(pl.Backfill.Do().DML())
	}

	err = ch.ExecuteWitchLogger(pl.Backfill.Do().DML())
	fmt.Println("")
	return err
}

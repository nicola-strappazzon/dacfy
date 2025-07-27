package populate

import (
	"github.com/nicola-strappazzon/clickhouse-dac/clickhouse"
	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"
	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "populate",
		Short:   "Populate tables as defined in the pipelines.",
		Long:    ``,
		Example: `dac populate --host=demo.clickhouse.cloud --user=default --password=mypass --pipe=foo.yaml`,
		Run: func(cmd *cobra.Command, args []string) {
			Run()
		},
	}

	cmd.Flags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	cmd.MarkFlagRequired("pipe")

	return cmd
}

func Run() {
	if err := ch.ExecuteWitchLogger(pl.Populate().DML()); err != nil {
		panic(err)
	}
}

package populate

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
		Use:     "populate",
		Short:   "Populate tables as defined in the pipelines.",
		Example: `clickhouse-dac populate --pipe=foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	cmd.MarkFlagRequired("pipe")

	return cmd
}

func Run() error {
	if strings.IsNotEmpty(pl.Table.Name) {
		fmt.Println("--> Populate table:", pl.Table.Name)
	}

	if strings.IsNotEmpty(pl.View.To) {
		fmt.Println("--> Populate table:", pl.View.To)
	}

	return ch.ExecuteWitchLogger(pl.Populate().DML())
}

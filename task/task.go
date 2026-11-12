package task

import (
	"errors"
	"os"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "task <name> <pipeline.yaml>",
		Short:   "Run one task as defined in pipelines.",
		Example: "dacfy task sync foo.yaml",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 2 {
				return errors.New("missing task name or YAML file")
			}
			return cobra.ExactArgs(2)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runner{out: os.Stdout, pipeline: pl, execute: ch}.Run(args[0])
		},
	}
}

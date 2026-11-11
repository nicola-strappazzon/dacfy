package test

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
	var name string
	cmd := &cobra.Command{
		Use:     "test <pipeline.yaml>",
		Short:   "Test transformations as defined in pipelines.",
		Example: "dacfy test foo.yaml",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("missing YAML file")
			}
			return cobra.ExactArgs(1)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(name)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Run only the test with this name.")

	return cmd
}

func Run(name string) error {
	return runner{out: os.Stdout, pipeline: pl, query: ch, name: name}.Run()
}

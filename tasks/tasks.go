package tasks

import (
	"errors"
	"os"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/spf13/cobra"
)

var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "tasks <pipeline.yaml>",
		Short:   "List the tasks defined in pipelines.",
		Example: "dacfy tasks foo.yaml",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("missing YAML file")
			}
			return cobra.ExactArgs(1)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return lister{out: os.Stdout, tasks: pl.Tasks}.List()
		},
	}
}

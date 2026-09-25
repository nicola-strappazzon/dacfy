package graph

import (
	"fmt"
	"io"

	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use: "graph <pipeline.yaml> [...]", Short: "Print the pipeline dependency graph.",
		Example: "dacfy graph examples/splitview.yaml", Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error { return Run(cmd.OutOrStdout(), args) },
	}
}

func Run(out io.Writer, paths []string) error {
	pls := &pipelines.Pipelines{}
	err := pls.LoadFile(paths[0])
	tbl := pls.View.SourceTables()

	if tbl.Count() == 1 {
		fmt.Printf("- %s -> %s -> %s\n", tbl.First().Name, pls.View.Name, pls.View.To)
	}

	// Check t.First().Name is a file and load and use recursive.

	return err
}

package query

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
		Use:     "query",
		Short:   "Execute queries as defined in pipelines.",
		Example: `dacfy query foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	return cmd
}

func Run() (err error) {
	if pl.Table.Query.IsEmpty() && pl.View.Query.IsEmpty() {
		return
	}

	if pl.Table.Query.IsNotEmpty() {
		fmt.Println("--> Running table query:")
		fmt.Println(pl.Table.Query.ToString())
		if err = ch.Execute(pl.Database.Use().SQL(), false); err != nil {
			return err
		}

		if err = ch.Execute(pl.Table.Query.ToString(), true); err != nil {
			return err
		}

		fmt.Println("")
	}

	if pl.View.Query.IsNotEmpty() {
		fmt.Println("--> Running view query:")
		fmt.Println(pl.Table.Query.ToString())
		if err = ch.Execute(pl.Database.Use().SQL(), false); err != nil {
			return err
		}

		if pl.Config.SQL {
			fmt.Println(pl.View.Query.ToString())
		}

		if pl.Config.DryRun {
			return nil
		}

		if err = ch.Execute(pl.View.Query.ToString(), true); err != nil {
			return err
		}

		fmt.Println("")
	}

	return err
}

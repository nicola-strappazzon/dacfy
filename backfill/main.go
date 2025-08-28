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
		Example: `dacfy backfill foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	return cmd
}

func Run() (err error) {
	if err = pl.Backfill.Validate(); err != nil {
		return err
	}

	fmt.Printf("--> Starting backfill from view %s into table %s.\n\r", pl.View.Name, pl.View.To)

	if pl.Config.SQL {
		fmt.Println(pl.Backfill.Do().DML())
	}

	err = ch.Execute(pl.Backfill.Do().DML(), true)
	fmt.Println("")
	return err
}

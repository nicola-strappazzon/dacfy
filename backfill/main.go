package backfill

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/strings"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

var truncate bool

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "backfill",
		Short:   "Backfill tables as defined in the pipelines.",
		Example: `dacfy backfill foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().BoolVarP(&truncate, "truncate", "t", false, "Truncate the table before execution (this will delete all data)")

	return cmd
}

func Run() (err error) {
	if err = pl.Backfill.Validate(); err != nil {
		return err
	}

	pl.Table.Name = pl.View.To

	queries := []struct {
		Message   string
		Statement string
		Progress  bool
		Execute   bool
	}{
		{
			Statement: pl.Table.SetSuffix(pl.Config.Suffix).Truncate().SQL(),
			Message: fmt.Sprintf(
				"Truncate table: %s", pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString()),
			Execute: truncate,
		},
		{
			Statement: pl.Backfill.Suffix(pl.Config.Suffix).Do().SQL(),
			Message: fmt.Sprintf(
				"Starting backfill from view %s into table %s",
				pl.View.Name.Suffix(pl.Config.Suffix).ToString(),
				pl.View.To.Suffix(pl.Config.Suffix).ToString()),
			Progress: true,
			Execute:  true,
		},
	}

	for _, query := range queries {
		if !query.Execute {
			continue
		}

		if strings.IsEmpty(query.Statement) {
			continue
		}

		if strings.IsNotEmpty(query.Message) {
			fmt.Println("-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Println(query.Statement + ";")
		}

		if pl.Config.DryRun {
			continue
		}

		if err := ch.Execute(query.Statement, query.Progress); err != nil {
			return err
		}
	}

	fmt.Println("")
	return nil
}

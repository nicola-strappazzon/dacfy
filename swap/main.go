package swap

import (
	"fmt"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/strings"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "swap",
		Short:   "Replace by renaming a table or view using the suffix.",
		Example: `dacfy swap foo.yaml --suffix _tmp`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if suffix, _ := cmd.Flags().GetString("suffix"); suffix == "" {
				cmd.Help()
				return fmt.Errorf(`required flag "suffix" not set.`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	return cmd
}

func Run() (err error) {
	if err = pl.Database.Validate(); err != nil {
		return err
	}

	if err = pl.Table.Validate(); err != nil {
		return err
	}

	if err = pl.View.Validate(); err != nil {
		return err
	}

	suffix := NowSuffix()
	queries := []struct {
		Message   string
		Statement string
	}{
		{
			Statement: pl.Table.Rename(pl.Table.Name.ToString(), pl.Table.Name.Suffix(suffix).ToString()).SQL(),
			Message:   fmt.Sprintf("Rename table: %s to %s.", pl.Table.Name.ToString(), pl.Table.Name.Suffix(suffix).ToString()),
		},
		{
			Statement: pl.Table.Rename(pl.Table.Name.Suffix(pl.Config.Suffix).ToString(), pl.Table.Name.ToString()).SQL(),
			Message:   fmt.Sprintf("Rename table: %s to %s.", pl.Table.Name.Suffix(pl.Config.Suffix).ToString(), pl.Table.Name.ToString()),
		},
		{
			Statement: pl.View.Rename(pl.View.Name.ToString(), pl.View.Name.Suffix(suffix).ToString()).SQL(),
			Message:   fmt.Sprintf("Rename view: %s to %s.", pl.View.Name.ToString(), pl.View.Name.Suffix(suffix).ToString()),
		},
		{
			Statement: pl.View.Rename(pl.View.Name.Suffix(pl.Config.Suffix).ToString(), pl.View.Name.ToString()).SQL(),
			Message:   fmt.Sprintf("Rename view: %s to %s.", pl.View.Name.Suffix(pl.Config.Suffix).ToString(), pl.View.Name.ToString()),
		},
	}

	for _, query := range queries {
		if strings.IsEmpty(query.Statement) {
			continue
		}

		if strings.IsNotEmpty(query.Message) {
			fmt.Println("-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Println(query.Statement)
		}

		if pl.Config.DryRun {
			continue
		}

		if err := ch.Execute(query.Statement, false); err != nil {
			return err
		}
	}

	return nil
}

func NowSuffix() string {
	return time.Now().Format("_20060102150405")
}

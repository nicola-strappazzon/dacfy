package swap

import (
	"fmt"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/gather"
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

func Dependency() error {
	tables := gather.Tables{}
	if err := tables.Load(pl.Database.Name.ToString()); err != nil {
		return err
	}

	for _, table := range tables {
		if table.Dependencies.Tables.IsNotEmpty() {
			return fmt.Errorf(
				"Cannot run swap command, the table %s is referenced by views: %v. Please drop the views first before continuing.",
				pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString(),
				table.Dependencies.Tables,
			)
		}
	}

	return nil
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

	if err = Dependency(); err != nil {
		return err
	}

	suffix_set := pl.Config.Suffix
	suffix_tmp := NowSuffix()
	pl.Config.Suffix = ""

	queries := []struct {
		Message   string
		Statement string
	}{
		{
			Statement: pl.View.Drop().SQL(),
			Message:   fmt.Sprintf("Drop view: %s", pl.View.Name.ToString()),
		},
		{
			Statement: pl.View.SetSuffix(suffix_set).Drop().SQL(),
			Message:   fmt.Sprintf("Drop view: %s", pl.View.Name.ToString()),
		},
		{
			Statement: pl.Table.Rename(pl.Table.Name.Suffix(suffix_tmp).ToString()).SQL(),
			Message:   fmt.Sprintf("Rename table: %s to %s", pl.Table.Name.ToString(), pl.Table.Name.Suffix(suffix_tmp).ToString()),
		},
		{
			Statement: pl.Table.SetSuffix(suffix_set).Rename(pl.Table.Name.ToString()).SQL(),
			Message:   fmt.Sprintf("Rename table: %s to %s", pl.Table.SetSuffix(suffix_set).Name.ToString(), pl.Table.Name.ToString()),
		},
		{
			Statement: pl.View.Create().SQL(),
			Message:   fmt.Sprintf("Create view: %s", pl.View.Name.ToString()),
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
			fmt.Println(query.Statement + ";")
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

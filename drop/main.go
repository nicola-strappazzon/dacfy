package drop

import (
	"fmt"

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
		Use:     "drop",
		Short:   "Remove tables and materialized views as defined in the pipelines.",
		Example: `dacfy drop foo.yaml`,
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

	table := tables.Get(pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString())

	if table.Dependencies.Tables.IsNotEmpty() {
		return fmt.Errorf(
			"Cannot run drop command, the table %s is referenced by views: %v. Please drop the views first before continuing.",
			pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString(),
			table.Dependencies.Tables,
		)
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

	queries := []struct {
		Message   string
		Statement string
		Delete    bool
	}{
		{
			Statement: pl.View.SetSuffix(pl.Config.Suffix).Drop().SQL(),
			Delete:    pl.View.Delete,
			Message:   fmt.Sprintf("Delete view: %s", pl.View.Name.Suffix(pl.Config.Suffix).ToString()),
		},
		{
			Statement: pl.Table.SetSuffix(pl.Config.Suffix).Drop().SQL(),
			Delete:    pl.Table.Delete,
			Message:   fmt.Sprintf("Delete table: %s", pl.Table.Name.Suffix(pl.Config.Suffix).ToString()),
		},
		{
			Statement: pl.Database.Drop().SQL(),
			Delete:    pl.Database.Delete,
			Message:   fmt.Sprintf("Delete database: %s", pl.Database.Name),
		},
	}

	for _, query := range queries {
		if !query.Delete {
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

		if err := ch.Execute(query.Statement, false); err != nil {
			return err
		}
	}

	return nil
}

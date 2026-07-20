package create

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/strings"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "create",
		Short:   "Create tables and materialized views as defined in the pipelines.",
		Example: `dacfy create foo.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(cmd)
		},
	}

	return cmd
}

func Run(cmd *cobra.Command) (err error) {
	if err = pl.Database.Validate(); err != nil {
		return err
	}

	if err = pl.Table.Validate(); err != nil {
		return err
	}

	if err = pl.View.Validate(); err != nil {
		return err
	}

	if err = pl.User.Validate(); err != nil {
		return err
	}

	for _, item := range pl.Table.Require {
		db, name := pl.Table.ParseRequireItem(item)
		if !ch.TableExists(db, name) {
			return fmt.Errorf("required object %q does not exist", item)
		}
	}

	if pl.User.IsNotEmpty() && !pl.Config.DryRun && !ch.DatabaseExists(pl.Database.Name.ToString()) {
		return fmt.Errorf("database %q does not exist", pl.Database.Name.ToString())
	}

	createDatabase := pl.Table.IsEmpty() && pl.View.IsEmpty() && pl.User.IsEmpty()

	queries := []struct {
		Message   string
		Statement string
		Continue  bool
	}{
		{
			Continue:  !createDatabase || (!pl.Config.DryRun && ch.DatabaseExists(pl.Database.Name.ToString())),
			Statement: pl.Database.Create().SQL(),
			Message:   fmt.Sprintf("Create database: %s", pl.Database.Name.ToString()),
		},
		{
			Statement: pl.Database.Use().SQL(),
		},
		{
			Continue:  !pl.Config.DryRun && ch.TableExists(pl.Database.Name.ToString(), pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString()),
			Statement: pl.Table.SetSuffix(pl.Config.Suffix).Create().SQL(),
			Message:   fmt.Sprintf("Create table: %s", pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString()),
		},
		{
			Continue:  !pl.Config.DryRun && ch.TableExists(pl.Database.Name.ToString(), pl.View.SetSuffix(pl.Config.Suffix).Name.ToString()),
			Statement: pl.View.SetSuffix(pl.Config.Suffix).Create().SQL(),
			Message:   fmt.Sprintf("Create view: %s", pl.View.SetSuffix(pl.Config.Suffix).Name.ToString()),
		},
	}

	if pl.User.IsNotEmpty() {
		queries = append(queries, struct {
			Message   string
			Statement string
			Continue  bool
		}{
			Statement: pl.User.Create().SQL(),
			Message:   fmt.Sprintf("Create user: %s", pl.User.Name.ToString()),
		})

		for _, grant := range pl.User.Grants {
			queries = append(queries, struct {
				Message   string
				Statement string
				Continue  bool
			}{
				Statement: pl.User.Grant(grant).SQL(),
				Message:   fmt.Sprintf("Grant %s on %s to %s", grant.Privilege, grant.On, pl.User.Name.ToString()),
			})
		}
	}

	for _, query := range queries {
		if query.Continue {
			continue
		}

		if strings.IsEmpty(query.Statement) {
			continue
		}

		if !pl.Config.DryRun && strings.IsNotEmpty(query.Message) {
			fmt.Fprintln(cmd.OutOrStdout(), "-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Fprintln(cmd.OutOrStdout(), query.Statement+";")
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

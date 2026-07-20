package create

import (
	"fmt"
	"path/filepath"

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

func Run(cmd *cobra.Command) error {
	created := map[string]bool{}

	if len(pl.Pipelines) > 0 {
		base := filepath.Dir(pl.Config.Pipe)
		files := pl.Pipelines

		for _, file := range files {
			if err := pl.LoadFile(filepath.Join(base, file)); err != nil {
				return err
			}

			if err := run(cmd, created); err != nil {
				return err
			}
		}

		return nil
	}

	return run(cmd, created)
}

func run(cmd *cobra.Command, created map[string]bool) (err error) {
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

		// Satisfied if an earlier pipeline of this run creates it, or it
		// already exists on the server.
		if created[db+"."+name] || ch.TableExists(db, name) {
			continue
		}

		// In dry-run nothing is executed, so we cannot confirm objects
		// against the server: warn instead of failing.
		if pl.Config.DryRun {
			fmt.Fprintf(cmd.ErrOrStderr(), "--> WARNING: required object %q does not exist\n", item)
			continue
		}

		return fmt.Errorf("required object %q does not exist", item)
	}

	if pl.Table.IsNotEmpty() {
		created[pl.Database.Name.ToString()+"."+pl.Table.Name.ToString()] = true
	}

	if pl.View.IsNotEmpty() {
		created[pl.Database.Name.ToString()+"."+pl.View.Name.ToString()] = true
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
			Continue:  pl.Table.IsEmpty() && pl.View.IsEmpty(),
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

		if !(pl.Config.DryRun && pl.Config.SQL) && strings.IsNotEmpty(query.Message) {
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

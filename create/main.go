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

	queries := []struct {
		Message   string
		Statement string
		Logger    bool
		Ignore    bool
	}{
		{
			Ignore:    ch.DatabaseExists(pl.Database.Name.ToString()),
			Statement: pl.Database.Create().DML(),
			Message:   fmt.Sprintf("Create database: %s", pl.Database.Name),
		},
		{
			Statement: pl.Database.Use().DML(),
		},
		{
			Statement: pl.Table.Create().DML(),
			Message:   fmt.Sprintf("Create table: %s", pl.Table.Name),
		},
		{
			Statement: pl.Table.Query.ToString(),
			Logger:    true,
		},
		{
			Statement: pl.View.Create().DML(),
			Message:   fmt.Sprintf("Create view: %s", pl.View.Name),
		},
	}

	for _, query := range queries {
		if strings.IsEmpty(query.Statement) {
			continue
		}

		if query.Ignore {
			continue
		}

		if strings.IsNotEmpty(query.Message) {
			fmt.Println("-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Println(query.Statement)
		}

		if err := ch.Execute(query.Statement, query.Logger); err != nil {
			return err
		}

		if query.Logger {
			fmt.Println("")
		}
	}

	return nil
}

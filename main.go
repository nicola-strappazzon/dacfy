package main

import (
	"github.com/nicola-strappazzon/dacfy/backfill"
	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/create"
	"github.com/nicola-strappazzon/dacfy/drop"
	"github.com/nicola-strappazzon/dacfy/human"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/terminal"
	"github.com/nicola-strappazzon/dacfy/version"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()
var tt = terminal.Terminal{}

type progressHandler struct{}

func init() {
	ch.SetLogger(progressHandler{})
}

func main() {
	var rootCmd = &cobra.Command{
		Use: "dacfy [COMMANDS] [OPTIONS]",
		Long: `ClickHouse Data as Code - A simple way to use pipelines for data transformation.

  You can define your databases, tables, materialized views, and populate or 
backfill them, all in a single step using a YAML file. Then, create everything
from the terminal and rollback just as easily, without effort or added complexity.

Find more information at: https://github.com/nicola-strappazzon/dacfy`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmd.Help()
				return
			}
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if cmd.Flags().Changed("pipe") {
				if err = pl.Load(pl.Config.Pipe); err != nil {
					return err
				}

				pl.SetParents()

				if err = pl.Database.Validate(); err != nil {
					return err
				}

				if err = pl.Table.Validate(); err != nil {
					return err
				}

				if err = pl.View.Validate(); err != nil {
					return err
				}

				return ch.Connect()
			}
			return nil
		},
		SilenceUsage: true,
	}

	rootCmd.PersistentFlags().StringVar(&pl.Config.Host, "host", "127.0.0.1:9000", "ClickHouse server host and port.")
	rootCmd.PersistentFlags().StringVar(&pl.Config.User, "user", "default", "Username for the ClickHouse server.")
	rootCmd.PersistentFlags().StringVar(&pl.Config.Password, "password", "", "Password for the ClickHouse server.")
	rootCmd.PersistentFlags().BoolVar(&pl.Config.TLS, "tls", false, "Enable TLS for the ClickHouse server.")
	rootCmd.PersistentFlags().BoolVar(&pl.Config.SQL, "sql", false, "Show SQL Statement.")
	rootCmd.PersistentFlags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	rootCmd.AddCommand(create.NewCommand())
	rootCmd.AddCommand(drop.NewCommand())
	rootCmd.AddCommand(backfill.NewCommand())
	rootCmd.AddCommand(version.NewCommand())

	rootCmd.Execute()
}

func (progressHandler) WriteProgress(in clickhouse.Progress) {
	tt.New()
	tt.Write("\r[%.0f%%] %d of %d Rows, %s, %2.2f CPU, %s RAM, Elapsed %s",
		in.Percent(),
		in.ReadRows,
		in.TotalRows,
		human.Bytes(in.ReadBytes),
		in.CPU,
		human.Bytes(in.Memory),
		human.Duration(in.Elapsed()),
	)
}

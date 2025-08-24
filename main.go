package main

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/backfill"
	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/create"
	"github.com/nicola-strappazzon/dacfy/drop"
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
			if pl.Config.Pipe == "" && len(args) > 0 {
				pl.Config.Pipe = args[0]

				if err = pl.Load(pl.Config.Pipe); err != nil {
					return err
				}

				pl.SetParents()

				return ch.Connect()
			}
			if pl.Config.Pipe == "" {
				return fmt.Errorf("missing YAML file")
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
	rootCmd.PersistentFlags().BoolVar(&pl.Config.Debug, "debug", false, "Enable debug mode.")
	rootCmd.AddCommand(create.NewCommand())
	rootCmd.AddCommand(drop.NewCommand())
	rootCmd.AddCommand(backfill.NewCommand())
	rootCmd.AddCommand(version.NewCommand())

	rootCmd.Execute()
}

func (progressHandler) WriteProgress(in clickhouse.Progress) {
	tt.New()
	tt.Write(in.ToString())
}

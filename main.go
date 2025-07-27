package main

import (
	// "fmt"

	"github.com/nicola-strappazzon/clickhouse-dac/clickhouse"
	"github.com/nicola-strappazzon/clickhouse-dac/deploy"
	"github.com/nicola-strappazzon/clickhouse-dac/destroy"
	"github.com/nicola-strappazzon/clickhouse-dac/human"
	"github.com/nicola-strappazzon/clickhouse-dac/pipelines"
	"github.com/nicola-strappazzon/clickhouse-dac/populate"
	"github.com/nicola-strappazzon/clickhouse-dac/terminal"
	"github.com/nicola-strappazzon/clickhouse-dac/version"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()
var tt = terminal.Terminal{}

type progressHandler struct{}

func main() {
	tt.New()
	tt.CursorHide()

	var rootCmd = &cobra.Command{
		Use: "dac [COMMANDS] [OPTIONS]",
		Long: `ClickHouse Data as Code - A simple way to use pipelines for data transformation.

  You can define your databases, tables, materialized views, and populate
them, all in a single step using a YAML file. Then, deploy everything from
the terminal and rollback just as easily, without effort or added complexity.

Find more information at: https://github.com/nicola-strappazzon/clickhouse-dac`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmd.Help()
				return
			}
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("pipe") {
				pl.Load(pl.Config.Pipe)

				if err := ch.Connect(); err != nil {
					panic(err)
				}
				ch.SetLogger(progressHandler{})
			}
		},
	}

	rootCmd.PersistentFlags().StringVar(&pl.Config.Host, "host", "127.0.0.1:9000", "ClickHouse server host and port.")
	rootCmd.PersistentFlags().StringVar(&pl.Config.User, "user", "default", "Username for the ClickHouse server.")
	rootCmd.PersistentFlags().StringVar(&pl.Config.Password, "password", "", "Password for the ClickHouse server.")
	rootCmd.PersistentFlags().BoolVar(&pl.Config.TLS, "tls", false, "Enable TLS for the ClickHouse server.")
	rootCmd.PersistentFlags().StringVar(&pl.Config.Pipe, "pipe", "", "Path to the pipelines file.")
	rootCmd.AddCommand(deploy.NewCommand())
	rootCmd.AddCommand(destroy.NewCommand())
	rootCmd.AddCommand(populate.NewCommand())
	rootCmd.AddCommand(version.NewCommand())
	rootCmd.Execute()

	tt.Rune('\n')
	tt.CursorShow()
}

func (progressHandler) Progress(in clickhouse.Progress) {
	tt.Write("--> Processing: %d of %d Rows, %s, %2.2f CPU, %s RAM, Progress: %.2f%%, Elapsed:%s",
		in.ReadRows,
		in.TotalRows,
		human.Bytes(in.ReadBytes),
		in.CPU,
		human.Bytes(in.Memory),
		in.Percent(),
		human.Duration(in.Elapsed()),
	)
}

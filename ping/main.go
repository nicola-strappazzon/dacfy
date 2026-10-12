package ping

import (
	"fmt"

	"github.com/nicola-strappazzon/dacfy/clickhouse"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "ping",
		Short: "Verify the ClickHouse connection.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := ch.Connect(); err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), "ClickHouse connection successful.")
			return nil
		},
	}
}

package version

import (
	"github.com/spf13/cobra"
)

const VERSION string = "0.0.0-beta.4"

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "version",
		Short: "Print version number",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Println(VERSION)
		},
	}

	return cmd
}

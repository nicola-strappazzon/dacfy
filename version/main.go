package version

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version is a const to have the latest version number for this code.
const VERSION string = "0.0.0-beta.3"

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "version",
		Short: "Print version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(VERSION)
		},
	}

	return cmd
}

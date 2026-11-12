package task

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var spec string
	cmd := &cobra.Command{
		Use:     "task <name> <pipeline.yaml>",
		Short:   "Run one task as defined in pipelines.",
		Example: "dacfy task sync foo.yaml\ndacfy task sync foo.yaml --cron \"*/5 0 * * *\"",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 2 {
				return errors.New("missing task name or YAML file")
			}
			if err := cobra.ExactArgs(2)(cmd, args); err != nil {
				return err
			}
			return validateCron(spec)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			r := runner{out: os.Stdout, pipeline: pl, execute: ch}
			if spec == "" || pl.Config.DryRun {
				return r.Run(args[0])
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			// After the first signal, the default handling comes back, so a
			// second Ctrl+C exits at once.
			context.AfterFunc(ctx, stop)
			return r.Schedule(ctx, args[0], spec)
		},
	}
	cmd.Flags().StringVar(&spec, "cron", "", `Run the task on a cron schedule until it is interrupted, for example "*/5 * * * *".`)

	return cmd
}

// validateCron checks the --cron value before dacfy connects to ClickHouse.
func validateCron(spec string) error {
	if spec == "" {
		return nil
	}
	if _, err := cron.ParseStandard(spec); err != nil {
		return fmt.Errorf("invalid --cron %q: %w", spec, err)
	}
	return nil
}

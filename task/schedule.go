package task

import (
	"context"
	"fmt"

	"github.com/robfig/cron/v3"
)

// Schedule runs the task with this name at each time of the cron schedule
// until ctx is canceled. A time is skipped while a run is still going, and a
// failed run does not stop the schedule. On cancel, it waits for a running
// task to finish.
func (r runner) Schedule(ctx context.Context, name, spec string) error {
	item, err := r.pipeline.Tasks.Find(name)
	if err != nil {
		return err
	}
	if _, err := item.SQL(r.table()); err != nil {
		return err
	}

	c := cron.New(cron.WithChain(cron.SkipIfStillRunning(cron.DiscardLogger)))
	if _, err := c.AddFunc(spec, func() {
		if err := r.Run(name); err != nil {
			fmt.Fprintf(r.out, "--> Task %s failed: %v\n", name, err)
		}
	}); err != nil {
		return fmt.Errorf("invalid --cron %q: %w", spec, err)
	}

	fmt.Fprintf(r.out, "--> Task %s scheduled with cron %q, press Ctrl+C to stop.\n", name, spec)
	c.Start()
	<-ctx.Done()
	<-c.Stop().Done()
	return nil
}

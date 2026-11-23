package task

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule runs the task with this name at each time of the cron schedule
// until ctx is canceled. A time is skipped while a run is still going, and a
// failed run does not stop the schedule. On cancel, it waits for a running
// task to finish. Each run writes one log line; --sql prints the
// statement once, before the schedule starts.
func (r runner) Schedule(ctx context.Context, name, spec string) error {
	statement, err := r.statement(name)
	if err != nil {
		return err
	}

	schedule, err := parseCron(spec)
	if err != nil {
		return err
	}

	c := cron.New(cron.WithChain(cron.SkipIfStillRunning(cron.DiscardLogger)))
	c.Schedule(schedule, cron.FuncJob(func() {
		if err := r.execute.Execute(statement, true); err != nil {
			fmt.Fprintf(r.out, "%s Task %s failed: %v\n", time.Now().Format(time.RFC3339), name, err)
		}
	}))

	r.printSQL(statement)
	fmt.Fprintf(r.out, "%s Task %s scheduled with cron %q, press Ctrl+C to stop.\n", time.Now().Format(time.RFC3339), name, spec)
	c.Start()
	<-ctx.Done()
	<-c.Stop().Done()
	return nil
}

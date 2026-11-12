package tasks

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/nicola-strappazzon/dacfy/pipelines"
)

type lister struct {
	out   io.Writer
	tasks pipelines.Tasks
}

// List writes one line per task with its name and its description.
func (l lister) List() error {
	if err := l.tasks.Validate(); err != nil {
		return err
	}
	w := tabwriter.NewWriter(l.out, 0, 0, 2, ' ', 0)
	for _, item := range l.tasks {
		fmt.Fprintf(w, "%s\t%s\n", item.Name.ToString(), item.Description)
	}
	return w.Flush()
}

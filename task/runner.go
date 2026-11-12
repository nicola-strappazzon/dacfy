package task

import (
	"fmt"
	"io"

	"github.com/nicola-strappazzon/dacfy/pipelines"
)

type runner struct {
	out      io.Writer
	pipeline *pipelines.Pipelines
	execute  interface {
		Execute(string, bool) error
	}
}

// Run runs the task with this name.
func (r runner) Run(name string) error {
	item, err := r.pipeline.Tasks.Find(name)
	if err != nil {
		return err
	}
	statement, err := item.SQL(r.table())
	if err != nil {
		return err
	}

	if !r.pipeline.Config.DryRun {
		fmt.Fprintf(r.out, "--> Running task: %s\n", item.Name.ToString())
	}
	if r.pipeline.Config.SQL {
		fmt.Fprintln(r.out, statement+";")
	}
	if r.pipeline.Config.DryRun {
		return nil
	}
	if err := r.execute.Execute(statement, true); err != nil {
		return err
	}
	fmt.Fprintln(r.out)
	return nil
}

// table returns the manifest table as database.table, with the suffix, or an
// empty string when the manifest has no table.
func (r runner) table() string {
	if r.pipeline.Table.Name.IsEmpty() {
		return ""
	}
	return r.pipeline.Database.Name.ToString() + "." + r.pipeline.Table.SetSuffix(r.pipeline.Config.Suffix).Name.ToString()
}

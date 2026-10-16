package test

import (
	"errors"
	"fmt"
	"io"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
)

type runner struct {
	out      io.Writer
	pipeline *pipelines.Pipelines
	query    interface {
		Query(string) (clickhouse.Result, error)
	}
}

func (r runner) Run() error {
	if len(r.pipeline.Tests) == 0 {
		return errors.New("no tests defined")
	}

	var errs []error
	for _, item := range r.pipeline.Tests {
		outcomes, err := r.RunTest(item)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if r.pipeline.Config.DryRun {
			continue
		}

		if err := r.writeOutcomes(item, outcomes); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (r runner) RunTest(item pipelines.Test) ([]outcome, error) {
	item = item.SetSuffix(r.pipeline.Config.Suffix)
	statement, err := item.SQL(r.pipeline.Database.Name)
	if err != nil {
		return nil, fmt.Errorf("test %q: %w", item.Name.ToString(), err)
	}
	if r.pipeline.Config.SQL || r.pipeline.Config.DryRun {
		fmt.Fprintln(r.out, statement+";")
	}
	if r.pipeline.Config.DryRun {
		return nil, nil
	}

	result, err := r.query.Query(statement)
	if err != nil {
		fmt.Fprintln(r.out, "--> Test error:", item.Name.ToString())
		return nil, fmt.Errorf("test %q: %w", item.Name.ToString(), err)
	}

	outcomes, err := r.evaluate(item, result)
	if err != nil {
		return nil, fmt.Errorf("test %q: %w", item.Name.ToString(), err)
	}
	return outcomes, nil
}

func (r runner) evaluate(item pipelines.Test, result clickhouse.Result) ([]outcome, error) {
	switch {
	case len(item.Unique) > 0:
		return (uniqueness{fields: key(item.Unique), result: result}).Outcomes()
	case len(item.Checks) > 0:
		return (checks{rules: item.Checks, result: result}).Outcomes()
	default:
		return (comparison{pipeline: item, result: result}).Outcomes()
	}
}

func (r runner) writeOutcomes(item pipelines.Test, outcomes []outcome) error {
	fmt.Fprintln(r.out, "--> Test:", item.Name.ToString())
	failed := false
	for _, outcome := range outcomes {
		fmt.Fprintln(r.out, outcome)
		if !outcome.pass {
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("test %q failed", item.Name.ToString())
	}
	return nil
}

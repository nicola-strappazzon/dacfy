package test

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
)

type runner struct {
	out      io.Writer
	pipeline *pipelines.Pipelines
	name     string
	query    interface {
		Query(string, clickhouse.Settings) (clickhouse.Result, error)
	}
}

func (r runner) Run() error {
	tests, err := r.selectedTests()
	if err != nil {
		return err
	}

	var errs []error
	for _, item := range tests {
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

// noCacheSettings turns off the caches that make a repeated test query faster
// than its first run, so durations measure real reads. A SETTINGS clause in the
// test query overrides them.
func noCacheSettings() clickhouse.Settings {
	return clickhouse.Settings{
		"use_query_cache":            0,
		"use_query_condition_cache":  0,
		"use_uncompressed_cache":     0,
		"min_bytes_to_use_direct_io": 1,
	}
}

// selectedTests returns all tests, or only the test named by --name.
func (r runner) selectedTests() ([]pipelines.Test, error) {
	if len(r.pipeline.Tests) == 0 {
		return nil, errors.New("no tests defined")
	}
	if r.name == "" {
		return r.pipeline.Tests, nil
	}
	for _, item := range r.pipeline.Tests {
		if item.Name.ToString() == r.name {
			return []pipelines.Test{item}, nil
		}
	}
	return nil, fmt.Errorf("test %q not found", r.name)
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

	start := time.Now()
	result, err := r.query.Query(statement, noCacheSettings())
	elapsed := time.Since(start)
	if err != nil {
		fmt.Fprintln(r.out, "--> Test error:", item.Name.ToString())
		return nil, fmt.Errorf("test %q: %w", item.Name.ToString(), err)
	}

	outcomes, err := r.evaluate(item, result, elapsed)
	if err != nil {
		return nil, fmt.Errorf("test %q: %w", item.Name.ToString(), err)
	}
	return outcomes, nil
}

func (r runner) evaluate(item pipelines.Test, result clickhouse.Result, elapsed time.Duration) ([]outcome, error) {
	switch {
	case item.Query != "":
		return (measurement{rows: item.Rows, maxDuration: item.MaxDuration, count: uint64(len(result.Rows)), elapsed: elapsed}).Outcomes()
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

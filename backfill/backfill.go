package backfill

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/strings"

	"github.com/spf13/cobra"
)

const (
	dateFormat   = "2006-01-02 15:04:05"
	defaultChunk = "1d"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

func NewCommand() *cobra.Command {
	var options pipelines.BackfillOptions

	var cmd = &cobra.Command{
		Use:   "backfill",
		Short: "Backfill tables as defined in the pipelines.",
		Example: `
  dacfy backfill foo.yaml
  dacfy backfill foo.yaml --from '2026-05-01 00:00:00' --to '2026-06-30 23:59:59'
  dacfy backfill foo.yaml --from '2026-05-01 00:00:00' --to '2026-06-30 23:59:59' --chunk 1d
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return NewRunner(cmd.OutOrStdout(), options).Run()
		},
	}

	cmd.Flags().BoolVarP(&options.Truncate, "truncate", "t", false, "Truncate the table before execution (this will delete all data)")
	cmd.Flags().StringVar(&options.From, "from", "", "Start datetime for chunk mode (YYYY-MM-DD HH:MM:SS)")
	cmd.Flags().StringVar(&options.To, "to", "", "End datetime for chunk mode (YYYY-MM-DD HH:MM:SS)")
	cmd.Flags().StringVar(&options.Chunk, "chunk", defaultChunk, "Chunk size: Nh (hours), Nd (days), NM (months)")

	return cmd
}

// Runner runs a backfill of the loaded pipeline with the given options and
// writes its messages to out.
type Runner struct {
	out     io.Writer
	options pipelines.BackfillOptions
}

// NewRunner returns a Runner. An empty chunk size uses the default chunk size.
func NewRunner(out io.Writer, options pipelines.BackfillOptions) Runner {
	if options.Chunk == "" {
		options.Chunk = defaultChunk
	}

	return Runner{out: out, options: options}
}

// Validate checks the options without running the backfill.
func (r Runner) Validate() error {
	if !r.isChunked() {
		return nil
	}

	_, _, _, _, err := r.chunkRange()
	return err
}

func (r Runner) Run() error {
	if err := r.Validate(); err != nil {
		return err
	}

	if r.isChunked() {
		return r.runChunked()
	}

	return r.runFull()
}

func (r Runner) isChunked() bool {
	return r.options.From != "" || r.options.To != ""
}

// chunkRange parses the date range and the chunk size.
func (r Runner) chunkRange() (from, to time.Time, n int, unit byte, err error) {
	if r.options.From == "" || r.options.To == "" {
		return from, to, n, unit, fmt.Errorf("--from and --to must both be specified")
	}

	if from, err = parseDate(r.options.From); err != nil {
		return from, to, n, unit, fmt.Errorf("--from: %w", err)
	}

	if to, err = parseDate(r.options.To); err != nil {
		return from, to, n, unit, fmt.Errorf("--to: %w", err)
	}

	if !from.Before(to) {
		return from, to, n, unit, fmt.Errorf("--from must be before --to")
	}

	n, unit, err = parseChunk(r.options.Chunk)
	return from, to, n, unit, err
}

func (r Runner) runFull() (err error) {
	if pl.View.Name.IsEmpty() && pl.Table.Query.IsNotEmpty() {
		return r.runFullFromTableQuery()
	}

	if err = pl.Backfill.Validate(); err != nil {
		return err
	}

	pl.Table.Name = pl.View.To

	queries := []struct {
		Message   string
		Statement string
		Progress  bool
		Execute   bool
	}{
		{
			Statement: pl.Table.SetSuffix(pl.Config.Suffix).Truncate().SQL(),
			Message: fmt.Sprintf(
				"Truncate table: %s", pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString()),
			Execute: r.options.Truncate,
		},
		{
			Statement: pl.Backfill.Suffix(pl.Config.Suffix).Do().SQL(),
			Message: fmt.Sprintf(
				"Starting backfill from view %s into table %s",
				pl.View.Name.Suffix(pl.Config.Suffix).ToString(),
				pl.View.To.Suffix(pl.Config.Suffix).ToString()),
			Progress: true,
			Execute:  true,
		},
	}

	for _, query := range queries {
		if !query.Execute {
			continue
		}

		if strings.IsEmpty(query.Statement) {
			continue
		}

		if !pl.Config.DryRun && strings.IsNotEmpty(query.Message) {
			fmt.Fprintln(r.out, "-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Fprintln(r.out, query.Statement+";")
		}

		if pl.Config.DryRun {
			continue
		}

		if err := ch.Execute(query.Statement, query.Progress); err != nil {
			return err
		}
	}

	fmt.Fprintln(r.out, "")
	return nil
}

func (r Runner) runFullFromTableQuery() (err error) {
	if !pl.Config.DryRun {
		fmt.Fprintln(r.out, "--> Starting backfill from table query into:", pl.Table.Name.ToString())
	}

	if pl.Config.SQL {
		fmt.Fprintln(r.out, pl.Table.Query.ToString()+";")
	}

	if pl.Config.DryRun {
		return nil
	}

	if err = ch.Execute(pl.Database.Use().SQL(), false); err != nil {
		return err
	}

	if err = ch.Execute(pl.Table.Query.ToString(), true); err != nil {
		return err
	}

	fmt.Fprintln(r.out, "")
	return nil
}

func (r Runner) runChunked() (err error) {
	if err = pl.Backfill.ValidateChunk(); err != nil {
		return err
	}

	fromTime, toTime, n, unit, err := r.chunkRange()
	if err != nil {
		return err
	}

	table := pl.Table
	if pl.View.Materialized {
		table.Name = pl.View.To
	}

	if r.options.Truncate {
		stmt := table.SetSuffix(pl.Config.Suffix).Truncate().SQL()
		if !pl.Config.DryRun {
			fmt.Fprintln(r.out, "--> Truncate table:", table.SetSuffix(pl.Config.Suffix).Name.ToString())
		}
		if !pl.Config.DryRun {
			if err := ch.Execute(stmt, false); err != nil {
				return err
			}
		}
	}

	total := countChunks(fromTime, toTime, n, unit)
	i := 1

	for current := fromTime; current.Before(toTime); {
		next := addChunk(current, n, unit)
		if next.After(toTime) {
			next = toTime
		}

		dateFrom := current.Format(dateFormat)
		dateTo := next.Format(dateFormat)

		if !pl.Config.DryRun {
			fmt.Fprintf(r.out, "--> Chunk %d/%d: %s => %s\n", i, total, dateFrom, dateTo)
		}

		q := pl.Backfill.Suffix(pl.Config.Suffix).DoChunk(dateFrom, dateTo)

		if pl.Config.SQL {
			fmt.Fprintln(r.out, q.SQL()+";")
		}

		if !pl.Config.DryRun {
			if err := ch.Execute(q.SQL(), true); err != nil {
				return fmt.Errorf("chunk %d/%d failed: %w", i, total, err)
			}
			fmt.Fprintln(r.out)
		}

		current = next
		i++
	}

	return nil
}

func parseDate(s string) (time.Time, error) {
	t, err := time.Parse(dateFormat, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid format %q, use YYYY-MM-DD HH:MM:SS", s)
	}
	return t, nil
}

func parseChunk(c string) (int, byte, error) {
	invalid := fmt.Errorf("invalid --chunk %q, use Nh (hours), Nd (days) or NM (months)", c)
	if len(c) < 2 {
		return 0, 0, invalid
	}
	n, err := strconv.Atoi(c[:len(c)-1])
	if err != nil || n <= 0 {
		return 0, 0, invalid
	}
	unit := c[len(c)-1]
	if unit != 'h' && unit != 'd' && unit != 'M' {
		return 0, 0, invalid
	}
	return n, unit, nil
}

func addChunk(t time.Time, n int, unit byte) time.Time {
	switch unit {
	case 'h':
		return t.Add(time.Duration(n) * time.Hour)
	case 'd':
		return t.AddDate(0, 0, n)
	default: // 'M'
		return t.AddDate(0, n, 0)
	}
}

func countChunks(from, to time.Time, n int, unit byte) int {
	count := 0
	for t := from; t.Before(to); t = addChunk(t, n, unit) {
		count++
	}
	return count
}

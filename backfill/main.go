package backfill

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nicola-strappazzon/dacfy/clickhouse"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/nicola-strappazzon/dacfy/strings"

	"github.com/spf13/cobra"
)

var ch = clickhouse.Instance()
var pl = pipelines.Instance()

var truncate bool
var from, to, chunk string

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     "backfill",
		Short:   "Backfill tables as defined in the pipelines.",
		Example: `dacfy backfill foo.yaml --from '2026-05-01 00:00:00' --to '2026-06-30 23:59:59' --chunk 1d`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run()
		},
	}

	cmd.Flags().BoolVarP(&truncate, "truncate", "t", false, "Truncate the table before execution (this will delete all data)")
	cmd.Flags().StringVar(&from, "from", "", "Start datetime for chunk mode (YYYY-MM-DD HH:MM:SS)")
	cmd.Flags().StringVar(&to, "to", "", "End datetime for chunk mode (YYYY-MM-DD HH:MM:SS)")
	cmd.Flags().StringVar(&chunk, "chunk", "1d", "Chunk size: Nh (hours), Nd (days), NM (months)")

	return cmd
}

func Run() (err error) {
	if (from == "") != (to == "") {
		return fmt.Errorf("--from and --to must both be specified")
	}

	if pl.Table.Backfill.Where != "" && from == "" {
		return fmt.Errorf("--from and --to are required when backfill.where is defined")
	}

	if from != "" && to != "" {
		return runChunked()
	}

	return runFull()
}

func runFull() (err error) {
	if pl.View.Name.IsEmpty() && pl.Table.Query.IsNotEmpty() {
		return runFullFromTableQuery()
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
			Execute: truncate,
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

		if strings.IsNotEmpty(query.Message) {
			fmt.Println("-->", query.Message)
		}

		if pl.Config.SQL {
			fmt.Println(query.Statement + ";")
		}

		if pl.Config.DryRun {
			continue
		}

		if err := ch.Execute(query.Statement, query.Progress); err != nil {
			return err
		}
	}

	fmt.Println("")
	return nil
}

func runFullFromTableQuery() (err error) {
	fmt.Println("--> Starting backfill from table query into:", pl.Table.Name.ToString())

	if pl.Config.SQL {
		fmt.Println(pl.Table.Query.ToString() + ";")
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

	fmt.Println("")
	return nil
}

func runChunked() (err error) {
	if err = pl.Backfill.ValidateChunk(); err != nil {
		return err
	}

	fromTime, dateFmt, err := parseDate(from)
	if err != nil {
		return fmt.Errorf("--from: %w", err)
	}

	toTime, _, err := parseDate(to)
	if err != nil {
		return fmt.Errorf("--to: %w", err)
	}

	if !fromTime.Before(toTime) {
		return fmt.Errorf("--from must be before --to")
	}

	pl.Table.Name = pl.View.To

	if truncate {
		stmt := pl.Table.SetSuffix(pl.Config.Suffix).Truncate().SQL()
		fmt.Println("--> Truncate table:", pl.Table.SetSuffix(pl.Config.Suffix).Name.ToString())
		if !pl.Config.DryRun {
			if err := ch.Execute(stmt, false); err != nil {
				return err
			}
		}
	}

	total := countChunks(fromTime, toTime, chunk)
	i := 1

	for current := fromTime; current.Before(toTime); {
		next := addChunk(current, chunk)
		if next.After(toTime) {
			next = toTime
		}

		dateFrom := current.Format(dateFmt)
		dateTo := next.Format(dateFmt)

		fmt.Printf("--> Chunk %d/%d: %s => %s\n", i, total, dateFrom, dateTo)

		q := pl.Backfill.Suffix(pl.Config.Suffix).DoChunk(dateFrom, dateTo)

		if pl.Config.SQL {
			fmt.Println(q.SQL() + ";")
		}

		if !pl.Config.DryRun {
			if err := ch.Execute(q.SQL(), true); err != nil {
				return fmt.Errorf("chunk %d/%d failed: %w", i, total, err)
			}
			fmt.Println()
		}

		current = next
		i++
	}

	return nil
}

func parseDate(s string) (time.Time, string, error) {
	const f = "2006-01-02 15:04:05"
	t, err := time.Parse(f, s)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid format %q, use YYYY-MM-DD HH:MM:SS", s)
	}
	return t, f, nil
}

func addChunk(t time.Time, c string) time.Time {
	if len(c) < 2 {
		return t.AddDate(0, 0, 1)
	}
	n, err := strconv.Atoi(c[:len(c)-1])
	if err != nil || n <= 0 {
		return t.AddDate(0, 0, 1)
	}
	switch c[len(c)-1] {
	case 'h':
		return t.Add(time.Duration(n) * time.Hour)
	case 'd':
		return t.AddDate(0, 0, n)
	case 'M':
		return t.AddDate(0, n, 0)
	default:
		return t.AddDate(0, 0, 1)
	}
}

func countChunks(from, to time.Time, chunk string) int {
	count := 0
	for t := from; t.Before(to); t = addChunk(t, chunk) {
		count++
	}
	return count
}

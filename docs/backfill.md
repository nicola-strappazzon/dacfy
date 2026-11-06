# Backfill

`dacfy backfill` fills the target table of a materialized view, or runs
`table.query`, as defined in the YAML file.

```bash
dacfy backfill foo.yaml
dacfy backfill foo.yaml --truncate
dacfy backfill foo.yaml --from '2026-05-01 00:00:00' --to '2026-06-30 23:59:59' --chunk 1d
dacfy backfill foo.yaml --detach
```

## Background execution

A synchronous backfill stops when the client connection ends or times out, and
ClickHouse cancels the query. Because `INSERT ... SELECT` is not atomic, the
target table may keep partial data; run it again with `--truncate`.

With `--detach`, dacfy submits the backfill with the ClickHouse setting
[`run_query_in_background=1`](https://clickhouse.com/docs/concepts/features/operations/background-queries)
and returns immediately. The server keeps running the query after the client
disconnects. `--truncate` still runs synchronously before the backfill is
submitted.

```text
--> Starting backfill from view mv_foo into table foo
--> Backfill running in background, query id: 0b9c...
    Running:  SELECT elapsed, read_rows, written_rows, memory_usage FROM system.processes WHERE query_id = '0b9c...';
    Finished: SELECT type, query_duration_ms, written_rows, exception FROM system.query_log WHERE query_id = '0b9c...' AND type != 'QueryStart';
```

Cancel it with `KILL QUERY WHERE query_id = '<query id>';`.

Limitations:

- The ClickHouse server must support `run_query_in_background`. dacfy checks
  `system.settings` before any statement runs, including `--truncate`, and
  stops with an error that shows the server version when the setting is
  missing. `--dry-run` skips this check.
- Submission succeeding does not mean the backfill succeeded. Check
  `system.query_log` for the final status and exception. On self-managed
  servers, `SYSTEM FLUSH LOGS query_log;` forces pending log entries.
- A server restart stops the query.
- `system.processes`, `system.query_log` and `KILL QUERY` are local to the node
  that accepted the query.
- `--detach` cannot be combined with `--from` and `--to`, because the chunks
  would run concurrently instead of one after another.

# Tasks

Declare named queries in `tasks` to keep them with the manifest, for example
`SYSTEM SYNC REPLICA` or `OPTIMIZE TABLE ... FINAL`. Tasks run only when you
call them by name.

```yaml
database:
  name: analytics
table:
  name: events
tasks:
  - name: sync
    description: Wait for the replica to get all parts.
    query: SYSTEM SYNC REPLICA {table}
  - name: optimize
    description: Merge all parts.
    query: |
      OPTIMIZE TABLE {table} FINAL
```

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Task name. Must be unique in the manifest. It starts with a letter and contains letters, digits, `_` or `-`. |
| `description` | No | Short text that `dacfy tasks` shows. |
| `query` | Yes | One SQL statement. A final `;` is removed. |

`{table}` is optional. dacfy replaces it with `<database>.<table>` from the
manifest, with `--suffix` applied. A query that uses `{table}` needs
`table.name`. Without `{table}`, write full table names in the query.

## List the tasks

```bash
dacfy tasks foo.yaml
```

It prints one line per task, in manifest order, with the name and the
description.
It does not connect to ClickHouse.

```text
sync      Wait for the replica to get all parts.
optimize  Merge all parts.
```

## Run a task

```bash
dacfy task sync foo.yaml
dacfy task optimize foo.yaml --suffix _v2 --sql
```

Only the named task runs. A name that is not in the manifest fails with
`task "<name>" not found`. The query runs as a statement, so result rows are
not printed. `--sql` prints the statement and `--dry-run` prints it without
running it.

## Run a task on a schedule

Add `--cron` to keep dacfy running and run the task at each scheduled time,
until you press Ctrl+C or the process gets SIGTERM:

```bash
dacfy task mongodb-sync foo.yaml --cron "*/5 0 * * *"
```

The value uses the standard cron syntax with five fields: minute, hour, day of
month, month and day of week. For example, `*/5 * * * *` runs every five
minutes, and `*/5 0 * * *` runs every five minutes between 00:00 and 00:55.
Descriptors such as `@hourly`, `@daily` and `@every 30s` are also accepted.
The schedule uses the local time zone; add a prefix such as
`CRON_TZ=UTC */5 * * * *` to use another one. dacfy reads the expression with
[robfig/cron](https://github.com/robfig/cron) and rejects an invalid one before
it connects to ClickHouse.

- The first run is at the first scheduled time, not when dacfy starts.
- If a run is still going at the next scheduled time, that time is skipped, so
  runs never overlap.
- If a run fails, dacfy prints `--> Task <name> failed: <error>` and keeps the
  schedule.
- On Ctrl+C or SIGTERM, dacfy stops the schedule and waits for a running task
  to finish. Press Ctrl+C again to exit at once.
- With `--dry-run`, dacfy checks the expression, prints the statement once, and
  exits.

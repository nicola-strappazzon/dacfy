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
    query: SYSTEM SYNC REPLICA {table}
  - name: optimize
    query: |
      OPTIMIZE TABLE {table} FINAL
```

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Task name. Must be unique in the manifest. |
| `query` | Yes | One SQL statement. A final `;` is removed. |

`{table}` is optional. dacfy replaces it with `<database>.<table>` from the
manifest, with `--suffix` applied. A query that uses `{table}` needs
`table.name`. Without `{table}`, write full table names in the query.

## List the tasks

```bash
dacfy tasks foo.yaml
```

It prints one line per task, in manifest order, with the name and the query.
It does not connect to ClickHouse.

```text
sync      SYSTEM SYNC REPLICA {table}
optimize  OPTIMIZE TABLE {table} FINAL
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

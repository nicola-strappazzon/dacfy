# Test existing data

Define `tests` in the same YAML as your pipeline, then run:

```bash
dacfy test pipeline.yaml
dacfy test pipeline.yaml --sql
dacfy test pipeline.yaml --dry-run
dacfy test pipeline.yaml --name malware_unique
dacfy test pipeline.yaml --no-cache
```

`--name` runs only the test with that name; an unknown name is an error.
`--sql` prints the queries before executing them. `--dry-run` validates the
tests and prints their SQL without connecting to ClickHouse. A missing file,
invalid test, query error, or failed assertion exits with a nonzero status.

By default, test queries use the normal server caches. With `--no-cache`, test
queries run without caches, so durations measure real reads and do not change
because a query ran before. dacfy then sends these settings with each test
query:

| Setting | Turns off |
|---|---|
| `use_query_cache = 0` | Stored query results. |
| `use_query_condition_cache = 0` | Stored filter matches from earlier runs. |
| `use_uncompressed_cache = 0` | Decompressed blocks kept in memory. |
| `min_bytes_to_use_direct_io = 1` | The OS page cache; data is read with direct I/O. |

The settings are not added to the SQL text, so `--sql` and `--dry-run` do not
show them. A `SETTINGS` clause in a `query` test overrides them. The mark cache
and the primary index stay in memory and cannot be turned off per query.
`use_query_condition_cache` requires ClickHouse 25.3 or later.

Use base table names in the manifest and pass `--suffix` to select a version:

```bash
dacfy test pipeline.yaml --suffix _v7
```

With `table: malware`, all test modes query `analytics.malware_v7`. The suffix
is appended literally to each test's table name; it does not replace an existing
version in the manifest or change the database name, field names, or `where`
expression. Without `--suffix`, the table name is used as written.

Each test uses one of four modes:

```yaml
database:
  name: analytics
tests:
  - name: malware_examples
    table: malware
    key: [id]
    expected:
      - { id: 101, score: 25 }
      - { id: 205, score: 7 }

  - name: malware_unique
    table: malware
    unique: [sha256, source]
    where: "source = 'vendor_a'"

  - name: malware_quality
    table: malware
    where: "source = 'vendor_a'"
    checks:
      - { field: sha256, not_null: true }
      - { field: score, not_null: true }
      - { field: score, between: [0, 100] }
      - { field: size, gt: 0 }

  - name: malware_lookup
    table: malware
    query: |
      SELECT sha256, score
      FROM {table}
      WHERE source = 'vendor_a' AND created_at >= '2026-01-01'
    rows: { gte: 1, lte: 1000 }
    max_duration: 2s
```

- `key` + `expected` checks the selected rows and reports each key in YAML order.
  Missing rows, repeated keys in the result, and different values fail. Expected
  rows must have the same columns and distinct keys. Values compare by text,
  allowing equivalent numeric types; `NULL` remains distinct from literal text.
  Columns that are not keys accept YAML lists for `Array` columns, including
  empty and nested lists. Lists compare item by item with the same rules. Key
  columns need single values.
- `unique` checks that each combination of the listed fields occurs at most once.
  Each duplicate group is reported with its row count. Nulls participate in grouping.
- `checks` counts violations for each rule in a single query. Each entry defines
  one rule: `not_null: true`, `between: [min, max]`, `gt`, `gte`, `lt`, or `lte`.
  `between` includes both bounds. Numeric rules skip nulls; add a separate
  `not_null` check when nulls must also fail. Bounds must be finite numbers.
- `query` runs your own SQL and reports how many rows it returns and how long
  it takes. The query must read the test table with `{table}`, which becomes
  `database.table` plus `--suffix`. `rows` is optional and takes `eq`, or `gte`
  and/or `lte`. `max_duration` is optional and uses Go duration syntax, such as
  `500ms`, `2s` or `1m`. Without them, the test only reports the values. The
  duration is the client wall time, from sending the query until the last row
  is read, so it includes the network and changes with the server load. All
  rows are read into memory; use `count()` or `LIMIT` for large results.
  `query` cannot be combined with `where` or `final`; write them in the query.

For example, with `Array(String)` columns:

```yaml
    key: [sha256]
    expected:
      - sha256: 0d6244a5e1c2
        sources_author_id: ["aba4f35a-3e8e-4770-8550-30a931eec66b"]
        sources_author_org_id: []
```

`final` is an optional boolean supported by the `expected`, `unique` and
`checks` modes and defaults to `false`. Set `final: true` to add `FINAL` after
the table name, before filtering and grouping, applying the table engine's merge
logic at query time. The engine must support `FINAL`; it is not detected
automatically. For example:

```yaml
tests:
  - name: malware_unique
    table: ds_mv_imp_malware
    final: true
    unique: [sha256]
```

`where` is an optional SQL expression supported by the `expected`, `unique`
and `checks` modes. Without a filter, `unique` and `checks` examine the whole
table. With no matching rows these two modes pass; use `expected` when
particular records must exist. Tests query existing data and do not create
tables or fixtures. An empty string is not `NULL`.

Example output:

```text
--> Test: malware_quality
    sha256 not_null: PASS
    score not_null: PASS
    score between [0, 100]: FAIL (3 rows)
    size > 0: PASS
--> Test: malware_lookup
    rows between [1, 1000]: PASS (37)
    duration <= 2s: FAIL (2.31s)
```

See [examples/tests.yaml](../examples/tests.yaml) for a complete manifest. Adjust its
database, table, and expected values to your existing dataset.

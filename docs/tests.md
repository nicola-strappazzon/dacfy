# Test existing data

Define `tests` in the same YAML as your pipeline, then run:

```bash
dacfy test pipeline.yaml
dacfy test pipeline.yaml --sql
dacfy test pipeline.yaml --dry-run
```

`--sql` prints the queries before executing them. `--dry-run` validates the tests
and prints their SQL without connecting to ClickHouse. A missing file, invalid
test, query error, or failed assertion exits with a nonzero status.

Use base table names in the manifest and pass `--suffix` to select a version:

```bash
dacfy test pipeline.yaml --suffix _v7
```

With `table: malware`, all test modes query `analytics.malware_v7`. The suffix
is appended literally to each test's table name; it does not replace an existing
version in the manifest or change the database name, field names, or `where`
expression. Without `--suffix`, the table name is used as written.

Each test uses one of three modes:

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
```

- `key` + `expected` checks the selected rows and reports each key in YAML order.
  Missing rows, repeated keys in the result, and different values fail. Expected
  rows must have the same columns and distinct keys. Values compare by text,
  allowing equivalent numeric types; `NULL` remains distinct from literal text.
- `unique` checks that each combination of the listed fields occurs at most once.
  Each duplicate group is reported with its row count. Nulls participate in grouping.
- `checks` counts violations for each rule in a single query. Each entry defines
  one rule: `not_null: true`, `between: [min, max]`, `gt`, `gte`, `lt`, or `lte`.
  `between` includes both bounds. Numeric rules skip nulls; add a separate
  `not_null` check when nulls must also fail. Bounds must be finite numbers.

`final` is an optional boolean supported by all three modes and defaults to
`false`. Set `final: true` to add `FINAL` after the table name, before filtering
and grouping, applying the table engine's merge logic at query time. The engine
must support `FINAL`; it is not detected automatically. For example:

```yaml
tests:
  - name: malware_unique
    table: ds_mv_imp_malware
    final: true
    unique: [sha256]
```

`where` is an optional SQL expression supported by all three modes. Without a
filter, `unique` and `checks` examine the whole table. With no matching rows these
two modes pass; use `expected` when particular records must exist. Tests query
existing data and do not create tables or fixtures. An empty string is not `NULL`.

Example output:

```text
--> Test: malware_quality
    sha256 not_null: PASS
    score not_null: PASS
    score between [0, 100]: FAIL (3 rows)
    size > 0: PASS
```

See [examples/tests.yaml](../examples/tests.yaml) for a complete manifest. Adjust its
database, table, and expected values to your existing dataset.

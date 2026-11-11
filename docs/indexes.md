# Table indexes

Declare data skipping indexes in `table.indexes`. `dacfy create` adds them
right after it creates the table, before the view and before any backfill, so
the indexes are built while the table is filled.

```yaml
database:
  name: malware_search
table:
  name: ds_mv_malware_search_file
  engine: ReplicatedMergeTree
  order_by: [updated_at, sha256]
  columns:
    - { name: updated_at, type: DateTime }
    - { name: sha256, type: String }
    - { name: search_file, type: String }
  indexes:
    - { name: idx_search_file, expression: search_file, type: "text(tokenizer = ngrams(3))", granularity: 1 }
    - { name: idx_sha256_bf, expression: sha256, type: bloom_filter(0.01), granularity: 1 }
```

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Index name. Must be unique in the table. |
| `expression` | Yes | Column or expression to index, for example `sha256` or `lower(path)`. |
| `type` | Yes | Index type, for example `minmax`, `set(100)`, `bloom_filter(0.01)` or `text(tokenizer = ngrams(3))`. Quote it when it contains `:` or `#`. |
| `granularity` | No | Number of granules per index block. Without it, the server default is used. |

Each index becomes one statement, in manifest order:

```sql
ALTER TABLE malware_search.ds_mv_malware_search_file ADD INDEX IF NOT EXISTS idx_search_file search_file TYPE text(tokenizer = ngrams(3)) GRANULARITY 1;
ALTER TABLE malware_search.ds_mv_malware_search_file ADD INDEX IF NOT EXISTS idx_sha256_bf sha256 TYPE bloom_filter(0.01) GRANULARITY 1;
```

- `--suffix` applies to the table name, `--sql` prints the statements, and
  `--dry-run` prints them without running them.
- The statements have no `ON CLUSTER`, like `CREATE TABLE`.
- When the table already exists, `dacfy create` skips it and its indexes.
- dacfy does not run `MATERIALIZE INDEX` and does not drop or change indexes
  on an existing table. To add an index to a table that already has data, run
  `ALTER TABLE ... ADD INDEX` and `ALTER TABLE ... MATERIALIZE INDEX` by hand.

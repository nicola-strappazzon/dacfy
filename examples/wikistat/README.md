# wikistat

This example is based on this post [Using Materialized Views in ClickHouse](https://clickhouse.com/blog/using-materialized-views-in-clickhouse) and demonstrates how to recreate it using `clickhouse-dac`.

This example shows how to create a materialized view by defining its target table, and how to manually populate it after creation.

```bash
cd examples/wikistat/
dac deploy --pipe=table.yaml
dac deploy --pipe=view.yaml
```

> [!NOTE]  
> Make sure the `clickhouse-dac` package is installed on your system.

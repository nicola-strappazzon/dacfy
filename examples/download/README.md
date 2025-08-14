# download

This example is based on this post [Materialized Views Illuminated, Part 1](https://altinity.com/blog/clickhouse-materialized-views-illuminated-part-1) and demonstrates how to recreate it using `dacfy`.

This example demonstrates how to create a materialized view without explicitly defining its target table. The destination table is created automatically, just like with the Autopopulate after create feature.

```bash
cd examples/download/
dacfy create --pipe=table.yaml
dacfy create --pipe=view.yaml
```

> [!NOTE]  
> Make sure the `dacfy` package is installed on your system.

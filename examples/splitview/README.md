# SplitView

SplitAgg is a ClickHouse example project showcasing how to use `AggregateFunction` with multiple independent materialized views derived from the same main events table.
The project simulates a realistic analytics workflow where each view processes a different subset of columns, allowing for parallel, specialized aggregations without interfering with each other.

```
        ┌──────────────┐
        │   events     │
        │ (main table) │
        └──────┬───────┘
               │
   ┌───────────┴────────┐
   │                    │
┌──▼─────────────┐   ┌──▼─────────────────┐
│ mv_user_daily  │   │ mv_revenue_country │
└──┬─────────────┘   └──┬─────────────────┘
   │                    │
┌──▼─────────────┐   ┌──▼─────────────────┐
│ agg_user_daily │   │ agg_revenue_country│
└────────────────┘   └────────────────────┘
```

It includes:

- A main table with random event data (~10 columns).
- Two materialized views, each focusing on half of the schema.
- Pre-aggregated metrics stored in AggregatingMergeTree tables for fast queries.

This setup can be adapted for use cases like engagement tracking, revenue analytics, or any scenario where the same event stream feeds multiple specialized aggregations.

## How to run

```bash
cd examples/splitview/
dacfy create events.yaml
dacfy create agg_user_daily.yaml
dacfy create agg_revenue_country.yaml
dacfy create mv_user_daily.yaml
dacfy create mv_revenue_country.yaml
```

> [!NOTE]  
> Make sure the `dacfy` package is installed on your system.

## Queries

### User engagement metrics

Retrieves per-user, per-device daily metrics such as event count, total amount spent, distinct event types, top 5 most frequent events, and spending percentiles.

```sql
SELECT
    user_id,
    device,
    finalizeAggregation(events_cnt)     AS events,
    finalizeAggregation(amount_sum)     AS amount_total,
    finalizeAggregation(uniq_events)    AS distinct_event_names,
    finalizeAggregation(top_events)     AS top5_event_names,
    finalizeAggregation(amount_q)       AS amount_p50_p90_p99
FROM agg_user_daily
WHERE event_date = toDate('2025-08-05')
ORDER BY amount_total DESC
LIMIT 10;
```

### Revenue by country and device

Returns daily revenue analytics per country and device, including unique users, total revenue, highest transaction, purchase count, and the top 10 spenders.


```sql
SELECT
    event_date,
    country,
    device,
    finalizeAggregation(users_uniq)     AS uniq_users,
    finalizeAggregation(rev_sum)        AS revenue_sum,
    finalizeAggregation(rev_max)        AS max_ticket,
    finalizeAggregation(purchases_cnt)  AS purchases,
    finalizeAggregation(rev_top_users)  AS top10_users
FROM agg_revenue_country
ORDER BY event_date, country, device
LIMIT 100;
```

### Multi-day aggregation using Merge combinators

Demonstrates how to re-aggregate pre-aggregated data over a date range, combining multiple days into a single result set for higher-level reporting.

```sql
SELECT
    country,
    sumMerge(rev_sum)                AS revenue_sum_all_days,
    uniqExactMerge(users_uniq)       AS uniq_users_all_days
FROM agg_revenue_country
WHERE event_date BETWEEN toDate('2025-08-01') AND toDate('2025-08-10')
GROUP BY country
ORDER BY revenue_sum_all_days DESC;
```

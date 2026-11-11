# Main pipelines file

A main file lists other pipeline files in `pipelines:`. Each entry is one step.
`dacfy create main.yaml` runs the steps in list order.

```yaml
pipelines:
  - ../malware-search/database.yaml
  - ds_imp_malware_jobs.yaml
  - cn_imp_malware_jobs.yaml
  - backfill: ds_imp_malware_jobs.yaml
    from: "2025-02-03 00:00:00"
    to: "2026-10-23 00:00:00"
    chunk: 1d
  - ds_mv_malware_jobs.yaml
  - vw_imp_malware_jobs.yaml
  - backfill: vw_imp_malware_jobs.yaml
```

This file runs the same steps as these commands:

```bash
dacfy create ../malware-search/database.yaml
dacfy create ds_imp_malware_jobs.yaml
dacfy create cn_imp_malware_jobs.yaml
dacfy backfill ds_imp_malware_jobs.yaml --from "2025-02-03 00:00:00" --to "2026-10-23 00:00:00" --chunk 1d
dacfy create ds_mv_malware_jobs.yaml
dacfy create vw_imp_malware_jobs.yaml
dacfy backfill vw_imp_malware_jobs.yaml
```

File paths are relative to the main file.

## Steps

- A file name creates that file.
- A `backfill` entry runs a backfill of that file. The backfill is a separate
  step, so you can put it after the files it reads from.

A `backfill` entry accepts the same options as the `backfill` command flags:

| Field      | Flag         | Description                                          |
|------------|--------------|------------------------------------------------------|
| `from`     | `--from`     | Start datetime for chunk mode (YYYY-MM-DD HH:MM:SS). |
| `to`       | `--to`       | End datetime for chunk mode (YYYY-MM-DD HH:MM:SS).   |
| `chunk`    | `--chunk`    | Chunk size: Nh (hours), Nd (days), NM (months). Default `1d`. |
| `truncate` | `--truncate` | Truncate the table before the backfill.              |
| `reverse`  | `--reverse`  | Run chunks from the newest to the oldest. Requires `from` and `to`. |

Without `from` and `to`, the step runs a full backfill.

## Behavior

- dacfy checks the options of all backfill steps before the first step runs. A
  wrong date or chunk size stops the run before anything is created.
- `--suffix`, `--dry-run` and `--sql` apply to all steps. Use
  `dacfy create main.yaml --dry-run --sql` to see all statements in order.
- `dacfy drop main.yaml` drops the files in reverse order and skips backfill
  steps.
- If a step fails, the run stops. Steps that already ran are not undone. To
  continue, run the remaining steps with `dacfy create` or `dacfy backfill`.

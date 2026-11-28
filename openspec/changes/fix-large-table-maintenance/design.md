## Context

`iceberg_maintenance` runs daily at 03:00 UTC and starts one SparkApplication per table with one driver and one executor. `dags/lake/manifest.py` sets pod resources of 1 CPU and 2 GiB, but no `spark.driver.memory` or `spark.executor.memory`; the Spark operator therefore sizes the containers from Spark's defaults (1 GiB heap plus 384 MiB overhead), and the pod template value is not what the pods get. Kernel logs on 2026-11-28 show `Memory cgroup out of memory: Killed process (java)` at about 1.3 GiB RSS for every attempt on `capture.raw` and `capture.events`.

Each run executes `rewrite_data_files`, `expire_snapshots` (older than `snapshot_max_age_days`, retain last 1) and `remove_orphan_files` (older than three days). Old `metadata.json` files are not referenced after expiry and are removed by the orphan cleanup.

## Decision drivers

- Maintenance must finish for tables with thousands of snapshots.
- Memory on the 32 GiB workers is tight during the day; maintenance runs at night, one table at a time.
- Reproducible training datasets must not depend on old snapshots.

## Considered options

1. **Explicit Spark memory, streamed expiry, one-day snapshot age for the capture tables (chosen).**
2. **Only more memory.** Fixes the failure, but with seven days of snapshots each table keeps about 6,000 snapshots and each `metadata.json` stays several MiB, so metadata keeps growing by GiBs per day between runs. Rejected.
3. **Expire in smaller batches over several runs.** More code, and still fails while the backlog exceeds the heap. Rejected.

## Decision

Option 1. Datasets that must stay reproducible are pinned by Iceberg tags (add-lake-layers, decision 13); tagged snapshots are not expired, so a one-day age is safe for the capture tables.

## Consequences

- Time travel on `capture.raw` and `capture.events` reaches back one day instead of seven, except for tagged snapshots.
- During maintenance one driver and one executor request 4 GiB each.
- The first successful run deletes about 8,700 snapshots per table and takes longer than later runs; the four-hour task timeout covers it.

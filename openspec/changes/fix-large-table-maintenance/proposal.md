## Why

Make the nightly maintenance of `capture.raw` and `capture.events` finish again. Every run of `iceberg_maintenance` since 2026-11-20 has failed for these two tables, while the eleven smaller tables succeed. The Spark driver and executor run with Spark's default 1 GiB heap (1.4 GiB container limit); the kernel OOM-kills them about 14 minutes into `expire_snapshots`, on every attempt.

Because maintenance never completes, nothing is expired or cleaned up. Corvus commits to each capture table about every 100 seconds, so each table holds about 8,700 snapshots and as many `metadata.json` files, each up to 4.8 MiB. On 2026-11-28 the warehouse held about 4 GiB of data files and about 45 GiB of metadata, almost all of it old `metadata.json` files of these two tables.

## What Changes

- New keys `driver_memory` and `executor_memory` in the `spark` section of `dags/config/maintenance.yaml` (Spark size strings such as `3g`), set to `3g` each. The SparkApplication sets `spark.driver.memory`, `spark.executor.memory` and a memory overhead of 1 GiB each, and the pod template requests and limits memory that equals heap plus overhead.
- New optional per-table key `snapshot_max_age_days`, overriding the global value. `capture.raw` and `capture.events` get `1`; all other tables keep the global `7`.
- `expire_snapshots` is called with `stream_results => true`, so the driver does not collect all deleted file paths at once.
- **Commit type and scope:** the implementation ships as `fix(dags)`.
- **Rollback:** revert the code PR. Expired snapshots are gone; that is accepted, see design.

## Out of scope

- Reducing how often Corvus commits, and Iceberg table properties such as `write.metadata.previous-versions-max`. Both belong to the Corvus repository, which owns and creates the capture tables.
- The bronze-to-silver layers (`add-lake-layers`).

## Dependencies (Kubernetes-FLUX)

None. Memory on the worker nodes is enough for one driver and one executor of 4 GiB each at night; tables are maintained one after another.

## Impact

- Capability `iceberg-maintenance` (see `specs/iceberg-maintenance/spec.md`).
- Files: `dags/config/maintenance.yaml`, `dags/lake/config.py`, `dags/lake/manifest.py`, `dags/lake/sql.py`, `dags/iceberg_maintenance.py` if it passes the per-table value, and their tests.
- Expected effect after the first successful run: `metadata.json` files of the two tables drop from about 8,700 to a few hundred each, and the bucket shrinks by about 40 GiB once orphan cleanup has run. Measured in the tasks, not assumed.

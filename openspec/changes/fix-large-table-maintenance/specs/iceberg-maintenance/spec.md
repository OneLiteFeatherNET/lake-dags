## ADDED Requirements

### Requirement: Maintenance Spark jobs are sized by configuration

THE maintenance SparkApplication SHALL set `spark.driver.memory` and `spark.executor.memory` from `driver_memory` and `executor_memory` in `dags/config/maintenance.yaml`, and its pods SHALL request and limit memory equal to heap plus overhead.

#### Scenario: Configured memory reaches the pods

- **GIVEN** `driver_memory: 3g` and `executor_memory: 3g`
- **WHEN** the manifest for a table is built
- **THEN** its Spark conf sets both memory values to `3g` and both pod templates request and limit 4 GiB

### Requirement: Snapshot age can be set per table

WHERE a table entry sets `snapshot_max_age_days`, THE maintenance SHALL expire that table's snapshots older than that many days instead of the global value.

#### Scenario: Capture table expires after one day

- **GIVEN** `capture.raw` has `snapshot_max_age_days: 1` and the global value is 7
- **WHEN** the statements are built at 2026-12-01 03:00 UTC
- **THEN** `expire_snapshots` for `capture.raw` uses `older_than => TIMESTAMP '2026-11-30 03:00:00'` and `stream_results => true`

#### Scenario: Large tables finish

- **GIVEN** the change is deployed
- **WHEN** the next nightly run of `iceberg_maintenance` completes
- **THEN** the tasks for `capture.raw` and `capture.events` succeed

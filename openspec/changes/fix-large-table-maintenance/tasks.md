## 1. Code (test first)

- [ ] 1.1 Tests for the parser: `driver_memory` and `executor_memory` accepted as Spark size strings, rejected when empty or malformed; per-table `snapshot_max_age_days` accepted as positive integer, absent means global value
- [ ] 1.2 Parse the new keys in `dags/lake/config.py` and set them in `dags/config/maintenance.yaml` (`3g`, `3g`; `capture.raw` and `capture.events` with `snapshot_max_age_days: 1`)
- [ ] 1.3 Tests and code in `dags/lake/manifest.py`: Spark conf memory and 1 GiB overhead, pod requests and limits equal heap plus overhead
- [ ] 1.4 Tests and code in `dags/lake/sql.py` and the DAG: per-table snapshot age, `stream_results => true`
- [ ] 1.5 Run the test suite

## 2. Rollout and verification

- [ ] 2.1 After merge, trigger `iceberg_maintenance` once for `capture.raw` and `capture.events` and confirm both tasks succeed
- [ ] 2.2 Count `metadata.json` files and bucket size of `polaris-corvus` before and after orphan cleanup

## 3. Pull request

- [ ] 3.1 Open the code pull request `fix(dags): size maintenance spark jobs for large tables`

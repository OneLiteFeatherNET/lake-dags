## 1. Verify assumptions before code

- [ ] 1.1 Schema of `capture.raw` and the other Corvus capture tables: facts recorded from Corvus origin/main on 2026-11-21 (design, verified facts; open question 1 resolved). Remaining: re-check the deployed tables before go-live
- [ ] 1.2 Confirm the deployed Airflow version (3.4.0 in CI and in Kubernetes-FLUX main, design open question 10 resolved) and that the pool `lake_spark` can be created
- [ ] 1.3 Spike on the cluster in a scratch namespace (never in `refined`, `features`, `labels`, `ref` or `quality`): branch write with `spark.wap.branch`, `fast_forward`, branch read, branch drop, a tag with `VERSION AS OF '<tag>'`, and a row delete by `player_key` followed by `expire_snapshots`, all through Polaris 1.9 with Iceberg 1.12. Record the results in design decisions 3 and 13
- [ ] 1.4 Privacy review before go-live: written purpose and review date for permanent `capture.raw` (chat and login names in plaintext), and the decision that the layers keep text out of silver and gold (design open question 9)
- [ ] 1.5 Open questions the code needs. Decided 2026-11-21: label entry as a CSV batch (open question 2, sub-points open), anti-xray source as a reviewed CSV (open question 13), erasure trigger option B (open question 14). Still open: Blackhole source and key mapping (open question 11; only an adapter interface ships), packet-name coverage (open question 5), ingest after erasure (open question 15), payload-less frames and seq gaps (open question 17). Questions that stay open keep the code behind a manual trigger

## 2. Platform dependencies (Kubernetes-FLUX, tracked there)

These items are not implemented in this repository. The code PR can merge before them, but no lake DAG runs in production until they exist.

- [ ] 2.1 Create the namespaces `refined`, `features`, `labels`, `ref` and `quality` in catalog `corvus`
- [ ] 2.2 Create the principal role `lake_pipeline` with table-level `TABLE_READ_DATA` on the five `capture` tables (`raw`, `events`, `sessions`, `segments`, `session_ends`), and create and write rights on `refined`, `features`, `labels`, `ref` and `quality`. No delete on any table in `capture`
- [ ] 2.3 Create the principal role `lake_erasure` with delete rights on the downstream tables (`refined`, `features`, `labels`, and the quarantine table in `quality`), read rights on the five `capture` tables for the guard, write rights on the status of `quality.erasure_requests`, and the maintenance grants for expire and orphan cleanup on the downstream namespaces. It holds no delete right on `capture`
- [ ] 2.4 Extend the grants of `lake_maintenance` and the read grants of `lake_sql` to the new namespaces. `lake_sql` gets no write rights
- [ ] 2.5 Add the Entra ID app roles `lake-pipeline` and `lake-erasure` to the API app "Polaris", create one client app per role with that role assigned, and store their client secrets as SOPS Secrets for the Airflow namespace
- [ ] 2.6 Create the Airflow pool `lake_spark` with 2 slots
- [ ] 2.7 Corvus side (Corvus repository, not this change): the erasure PR `feat(services): erase and retain capture data in iceberg tables` appends a pending row to `quality.erasure_requests` after its erasure of the five `capture` tables, which already exists, and closes ingest after erasure (open question 15, read `corvus.blocklist` at ingest). Corvus gets table-level write on that table only

## 3. Configuration (test first)

- [ ] 3.1 Write tests for the parser of `dags/config/layers.yaml`: unknown keys, identifiers, positive integers, blocking flags and thresholds, the split ratio, and the window size range
- [ ] 3.2 Add `dags/config/layers.yaml` (check thresholds from design decision 4, window size, split ratio and seed, retention proposal from decision 15) and its parser, following `dags/lake/config.py`
- [ ] 3.3 Change `capture.raw` in `dags/config/maintenance.yaml` to `retention_days: null` (permanent, design decision 15). This changes deployed behaviour that deletes raw after 21 days. Test the value
- [ ] 3.4 Add the derived tables to `maintenance.yaml` with `day_column` and `retention_days`, leaving the existing entries unchanged
- [ ] 3.5 Write a test that every table of design decision 15 appears in `maintenance.yaml` or `layers.yaml` with the expected retention

## 4. Pure logic (test first)

- [ ] 4.1 Check catalogue and verdict rules (`pass`, `warn`, `fail`, `overridden`, `not_available`), blocking and non-blocking, table and session scope, in `dags/lake/layers/checks.py`
- [ ] 4.2 Decoder `dags/lake/layers/decode.py` for the configuration-phase packets: VarInt length prefix, UTF-8 brand, NUL-separated channel list. Tests with byte fixtures, including multi-byte VarInts and an empty list
- [ ] 4.3 Silver SQL builders in `dags/lake/layers/refined.py`: dedup by `row_number` over (session_id, seq), `xxhash64` content hash, conflict detection, packet-name join on the version key, time delta from `offset_nanos`, session verdict, reconcile check, audit branch create, branch write, publish, drop, and stale branch cleanup. Tests assert structure, that no output table has a payload column, and that the statements never reference `capture` as a write target
- [ ] 4.4 Label rules in `dags/lake/layers/labels.py`: `untrusted` to `unknown`, `trusted` as negative, `cheater` with hack types, conflict to `unknown` with `labels.conflict`, module on and off windows, and the `MERGE INTO` upsert by `label_id`
- [ ] 4.5 Split assignment in `dags/lake/layers/split.py`: a deterministic function of `player_key` and the seed, with a test per property (stable, no player in both splits)
- [ ] 4.6 Gold builders in `dags/lake/layers/features.py`: session stats, token windows of 256 to 512 packets with the v1 token rule, `tokenizer_version`, `source_snapshot_id`, and the `features.build_runs` insert. Tests cover each token type and the window bounds
- [ ] 4.7 Erasure builders in `dags/lake/layers/erasure.py`: the pseudonym-filtered deletes for each downstream table in design decision 14, the guard reads of the five `capture` tables, the tag guard of decision 13, the status `MERGE` on `quality.erasure_requests`, and the expiry statements with the run time as `older_than`. Tests assert every delete is filtered by `player_key`, that no statement deletes from `capture`, and that a failed guard deletes nothing
- [ ] 4.8 Reference loader validation in `dags/lake/layers/reference.py`: states, directions, unique keys, and the `MERGE` for the affected protocol versions. Tests for the rejection of an invalid file, before any write

## 5. Spark job and integration test

- [ ] 5.1 Write `dags/lake/spark/layers_job.py`, self-contained like `maintenance_job.py`: it executes the statements it is given in order, and loads the decoder module that is shipped in the same ConfigMap. It does not import the `lake` package
- [ ] 5.2 Write the `spark`-marked integration test on a local file-based Iceberg catalog in a temp directory: a failing blocking check leaves `main` unchanged, a passing check publishes exactly the day partition, a rerun gives the same rows, and an erasure for one key removes it from the fixture tables
- [ ] 5.3 Set driver and executor resources, dynamic allocation bounds and `spark.sql.shuffle.partitions` as in design decision 16

## 6. DAGs

- [ ] 6.1 Write the DAG `lake_reference` (manual): reads the reviewed CSV files in `dags/config/reference/`, validates them with the loader, and runs the `MERGE`
- [ ] 6.2 Write the DAG `lake_refined`: schedule `0 4 * * *` for day D-1, asset outlet `iceberg://corvus/refined.packets`, `max_active_runs=1`, pool `lake_spark`, the write-audit-publish sequence of design decision 3 with cleanup in a `finally` step, `dates` and `force_publish` in the run conf
- [ ] 6.3 Write the DAG `lake_labels`: schedule `0 5 * * *`, asset outlet `iceberg://corvus/labels.windows`. It imports the admin CSV batches (design decision 10, option 1), and the Blackhole records through a source adapter interface with no concrete source until open question 11 is decided. It recomputes the last 30 days of label windows
- [ ] 6.4 Write the DAG `lake_features`: scheduled on `Asset(refined.packets) & Asset(labels.windows)`, reads the dates from the triggering events, builds the split, the session stats and the token windows for `v1`, writes `features.build_runs`
- [ ] 6.5 Write the DAG `lake_erasure`: scheduled hourly, processes every pending row of `quality.erasure_requests` in order, runs the guards and the steps of design decision 14, writes the status back, and reruns idempotently. A manual trigger with a `player_key` conf runs one request for a rerun
- [ ] 6.6 Add `pool="lake_spark"` to the maintenance tasks in `dags/iceberg_maintenance.py`
- [ ] 6.7 Add DAG import tests for all new DAGs and the changed maintenance DAG (marker `airflow`)

## 7. Runbooks (Outline, collection Infrastruktur)

- [ ] 7.1 Erasure runbook: the Corvus side (capture erasure, dataset tags) and the lake side (`lake_erasure`, the guards, the failure statuses and how to rerun), recorded per request
- [ ] 7.2 Labels runbook: how an admin batch is written, validated and imported, and how a controlled recording is recorded, with the label rules
- [ ] 7.3 Layers runbook: the schedules, the asset chain, `force_publish`, backfill conf, stale audit branches, and where to read `quality.check_results` in Superset

## 8. Validation

- [ ] 8.1 Run `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest -m "not airflow and not spark"` locally, and the full suite in CI
- [ ] 8.2 Run `openspec validate add-lake-layers --strict` once more before the code PR

## 9. First production runs

- [ ] 9.1 After the Kubernetes-FLUX dependencies exist, run `lake_reference`, then `lake_refined` for one past day, and record the result
- [ ] 9.2 For the first weeks, record the run duration and the peak memory of the driver and executors of each lake run in the layers runbook. A run over one hour, or a memory failure, is the trigger to raise the sizing (design decision 16)

## 10. Code pull request

- [ ] 10.1 Open the pull request `feat(dags): add refined, label and feature layers` from the implementation branch on the remote main; no files under `openspec/` in it

## Context

The lake from `add-data-lake` is deployed: catalog `corvus` on Polaris, bucket `polaris-corvus` on Ceph RGW, and the nightly `iceberg_maintenance` DAG in this repository. Corvus now writes `capture.raw`, the complete plaintext packet capture of both directions, with `player_key` as a pseudonym, and keeps it permanently. It also writes `capture.events`, a derived table, which gets `player_key` too. The maintainer's concept document (Minecraft-Paket-Lakehouse) defines the three tiers: bronze (raw, never changed), silver (decoded, cleaned, sorted per session) and gold (features and token sequences), plus labels and reference data.

Volume: at most 10 players per day, 1 to 4 hours each. That is 1.4 to 14 million packets and 1 to 15 GB per day, mostly server-to-client. Delivery is at least once, so duplicates on (session_id, seq) are expected, and late batches can land in a day that was already read.

## Verified facts (repository, 2026-11-18)

- `dags/iceberg_maintenance.py` runs at 03:00 UTC, one SparkApplication per table in `dags/config/maintenance.yaml`, with Spark 4.1.4 and Iceberg 1.12.0. Tables that do not exist are skipped. Retention is per table: `DELETE ... WHERE <day_column> < cutoff`. The current config keeps `capture.raw` 21 days. This change makes it permanent.
- `dags/lake/spark/maintenance_job.py` is self-contained and must not import the `lake` package. SQL is built by pure Python (`dags/lake/sql.py`).
- `pyproject.toml` pins `apache-airflow==3.4.0` for CI (checked on origin/main, 2026-11-21; this replaces the 3.3.2 pin noted on 2026-11-18). Kubernetes-FLUX main sets `airflowVersion` 3.4.0 (maintainer, 2026-11-21). The cluster check stays in task 1.2.
- The `lake_sql` change (Kubernetes-FLUX, `feat(data)`) gives staff read-only Trino access, with the Polaris principal `lake_sql`.
- The concept document, section "Ingest-Worker", states that the write path is at-least-once and that duplicates are removed in silver over (session_id, direction, seq). This change keys on (session_id, seq) instead, see the deviation below. The packet fields named there are ts, session_id, seq, player_key, direction, protocol_version, state, packet_id, backend, length, payload and payload_hash. The maintainer's column list for `capture.raw` uses `payload`, `direction`, `state`, `packet_type`, `wire_packet_id`, `protocol_version`, `player_key`, `seq` and `offset_nanos`. The two lists differ; the Corvus facts below fix the real schema.

## Verified facts from Corvus origin/main (2026-11-21)

Checked against Corvus origin/main and confirmed by the maintainer. They answer task 1.1 and open questions 1 and 4.

- `capture.raw` columns: `session_id` fixed(16), `segment` int, `seq` long, `offset_nanos` long, `direction` int, `state` int, `packet_type` long, `wire_packet_id` int, `protocol_version` int (nullable), `player_key` fixed(32) (nullable), `payload_original_size` long, `payload` binary (plaintext). Partition identity(`date`), where `date` is the UTC date of the session start. Sort (`session_id`, `seq`). Only frames with a payload get a raw row. Source: Corvus `services/src/corvus_services/schema.py:64-79`, `iceberg_sink.py:70-84`, `codec.py:140-164`.
- `offset_nanos` is nanoseconds since session start on a monotonic clock, not since the previous packet (Corvus `proto/capture.proto:110-111`). A time delta is therefore a difference of consecutive offsets within one direction.
- `packet_type` is Corvus' version-stable packet vocabulary id, 0 when unmapped. `wire_packet_id` is the version-dependent id on the wire (`capture.proto:114-117`).
- `direction`: 0 unspecified, 1 serverbound, 2 clientbound.
- `seq` is monotonic per session across both directions. A gap means a dropped frame (`capture.proto:92-96, 108-109`). The natural key is (`session_id`, `seq`), not (`session_id`, `direction`, `seq`).
- The backend server is not in `capture.raw`. It is `capture.segments.backend_server`, joined on (`session_id`, `segment`).
- Other Corvus tables now written:
  - `capture.sessions`: `session_id`, `player_key`, `pseudonym_key_id`, `client_protocol_version`, `observed_protocol_version`, `proxy_id`, `plugin_version`, `started_at`, `text_packets_captured`, `names_kept`.
  - `capture.segments`: `session_id`, `segment`, `backend_server`, `game_context`, `client_brand`, `started_at`, `cloudnet_task`, `cloudnet_groups`, `cloudnet_node`, `cloudnet_environment`, `stage`. It has no `player_key`.
  - `capture.session_ends`: `session_id`, `player_key`, `ended_at`, `dropped_queue_full`, `dropped_privacy`, `listener_errors`, `dropped_transport`. The counters are per message since the last batch, so the session total is the sum. A crashed session has no row.
- Redelivered session, segment and end messages append duplicate rows. Readers dedupe on `session_id` (and `segment`).
- `capture.events` has the Parquet event columns plus `player_key`. It has no protocol column.
- Erasure today: Corvus deletes the player's rows in all five `capture` tables with copy-on-write deletes, tags commits with the snapshot summary property `corvus.erasure-request`, and expires snapshots before the cut. Request state lives in Postgres `corvus.erasure_requests`, not in a lake table. Option B needs a Corvus change that appends to `quality.erasure_requests` (task 2.7).
- Known Corvus gap: ingest checks `players.erased_at` but never reads `corvus.blocklist`. A pseudonym erased before it was ever seen is not blocked at ingest (open question 15).

**Deviation from the concept's dedup key:** the concept uses (session_id, direction, seq). This change uses (session_id, seq). Reason: `seq` is unique per session across both directions, so `direction` adds nothing to the key, and a gap check per direction would report false gaps.

## Decision drivers

- The lake is the record for analysis. Raw is kept, so every silver and gold table must be rebuildable from it, and a new tokenizer or feature must be computable again.
- Nothing unchecked reaches a reader. A bad day stays out of silver and gold.
- Reruns are safe: a day can be recomputed without touching other days or tables.
- Data minimisation: plaintext chat and login names exist in bronze, but the layers copy only what they need.
- GDPR erasure reaches every table. Corvus erases `capture` itself, the lake erases the downstream layers, and both physical deletions are immediate.
- Training is reproducible: a dataset is pinned to snapshot ids, and it stays pinned unless erasure requires otherwise.
- Few moving parts: Airflow, Spark, Polaris and the existing test setup. No Trino writes, no Flink, no Kafka.
- The volume is small, so the sizing is set once and the operations stay simple.

## Options and decisions

### 1. Namespaces: deployed names plus the concept's tiers

**Options**

1. Keep the deployed `capture` (bronze) and add `refined` (silver), `features` (gold), `labels`, `ref` and `quality`. Chosen. The concept's `mc_*` names map as follows: `mc_bronze` to `capture`, `mc_silver` to `refined`, `mc_gold` to `features`, `mc_labels` to `labels`, `mc_ref` to `ref`.
2. Rename to the concept's `mc_*` names. Rejected: `capture` is already the namespace of the Corvus tables and the grants, the maintenance config, the `lake_sql` change and the Corvus writer all use it. A rename would touch a deployed writer for no gain.
3. A separate catalog `mc-lakehouse` as in the concept. Rejected: the catalog is already `corvus` on bucket `polaris-corvus`, and a second catalog would split the cross-layer joins and need a second set of credentials.

**Decision:** option 1. The namespace `ref` is added because the concept's packet-name and backend tables are reference data, not a layer. Every table name keeps the concept's name without the prefix (for example `refined.packets`, `features.session_stats`).

**Consequences:** the concept's names appear only in this document and in the runbooks, which map them. Polaris grants are per namespace, so each namespace gets its own write rights (Kubernetes-FLUX dependency).

### 2. Source of truth: the Corvus capture tables, read by the lake

**Decision:** the lake jobs read the Corvus tables and never write to them. `capture.raw` gives packet identity, direction, state, packet ids, timing and the payload. `capture.events` gives the decoded fields (movement, interactions and the other derived columns) that silver carries and that the session statistics use. `capture.sessions`, `capture.segments` and `capture.session_ends` give the session facts: client brand and version, the backend server, the disconnect and the dropped-packet counters. Both packet tables carry `player_key` and are keyed by (session_id, seq), as verified above. The silver build joins them on that key. Readers of the session tables dedupe on `session_id` (and `segment`), because redelivery appends duplicates. The pipeline role gets table-level read on all five tables, and no write on `capture`.

**Data minimisation inside the lake:** `refined.packets` carries no payload column. Payload is read from `capture.raw` only for the packet classes that need it (decision 5), and it is copied only into `refined.plugin_messages`. Chat, login, sign and book payloads are never copied into silver or gold. This keeps the concept's privacy recommendation for the derived layers, without changing the bronze decision.

**Consequences:** the silver build has a join on the key, which adds a shuffle (decision 16). Two checks protect the join: `rows.reconcile` against the distinct keys of `capture.raw`, and `events.coverage` in both directions (decision 4). `capture` stays owned by Corvus, which also erases it (decision 14).

### 3. Quality gate: Write-Audit-Publish with Iceberg branches

**Options**

1. Write the day to an audit branch of `refined.packets` and `refined.sessions`, run the checks on that branch, and `fast_forward` `main` only when no blocking check failed. Chosen.
2. Write to a staging table, then copy into `refined` after the checks. Rejected: two copies, and the copy is not atomic, so a failure between the steps leaves `refined` half-written.
3. Write to `main` and delete the day if a check fails. Rejected: readers see bad data between the write and the delete.

**Decision:** option 1. Per run `(table set, date)`:

1. Create the audit branch `audit_<run_id>` from `main`.
2. Write the day with `spark.wap.branch=audit_<run_id>` and dynamic partition overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`). Only partition `date = D` changes.
3. Run the checks on the branch (read as `refined.packets.branch_audit_<run_id>`). Write the results to `quality.check_results` on `main`, with the branch snapshot id.
4. If no blocking check failed: `CALL system.fast_forward(..., 'main', 'audit_<run_id>')`, then drop the branch. Otherwise drop the branch and leave `main` unchanged.
5. The branch is dropped in a `finally` step. A stale branch from an earlier failed run (prefix `audit_`, older than 1 day) is dropped at the start of the next run.

**Consequences:** `main` moves only through `fast_forward`. `lake_refined` runs with `max_active_runs=1`, so no second writer moves `main` between the write and the publish. The operator override `force_publish: true` publishes despite a blocking failure and records verdict `overridden`.

### 4. Checks and blocking rules

Checks are data in `dags/lake/layers/checks.py`: id, scope, metric, threshold and blocking flag. Metrics are computed in Spark SQL. Verdicts are `pass`, `warn` (non-blocking failure), `fail` (blocking failure) and `overridden`.

| Check | Scope | Blocking | Rule |
|---|---|---|---|
| `schema.capture_raw` | table | yes | Column names and types of `capture.raw` match the expected schema version. |
| `schema.capture_events` | table | yes | Column names and types of the event columns the lake reads from `capture.events` match the expected schema version. |
| `events.coverage` | table | no | Share of keys present in only one of `capture.raw` and `capture.events`, per day. Warn above 0.1 %. |
| `key.nulls` | table | yes | Null rate of `session_id`, `direction`, `seq`, `player_key`, `date` is 0. `player_key` is nullable in `capture.raw`, so the rule is enforced here. |
| `rows.reconcile` | table | yes | Distinct (session_id, seq) of the day in `capture.raw` equals the rows in `refined.packets` plus `row_count` of the day's quarantined sessions. |
| `rows.baseline` | table | yes | Distinct rows at least 50 % of the median of the 7 days before. Protects against an ingest outage. Override with `force_publish`. |
| `dup.rate` | table | no | Exact duplicate rate. Warn above 5 %. |
| `dup.conflict` | session | session | Same (session_id, seq) with a different content hash. Quarantines the session. |
| `seq.gaps` | session | session | Missing seq ratio per session, over the seq values of both directions (missing over max seq). Above 1 % quarantines that session. Table p99 is recorded. The concept runs this check per direction; this change does not, because `seq` spans both directions. Open question 17 covers payload-less frames, which are not in `capture.raw`. |
| `time.monotonic` | session | session | Share of timestamp decreases in seq order. Above 0.1 % quarantines the session. |
| `time.skew` | table | no | p50, p99 and max of the difference between server and client time, where both exist. Warn when p99 exceeds 5 s. The wall-clock source is open question 18. |
| `drops.reported` | session | no | Sum of `dropped_queue_full`, `dropped_privacy` and `dropped_transport` over the session's `capture.session_ends` rows. A session without such a row (crashed) gets `not_available`, which is recorded, not a failure. |
| `session.complete` | session | no | Share of sessions with a login and a `capture.session_ends` row (the disconnect). Incomplete sessions are flagged, not quarantined. |
| `payload.present` | table | no | Share of rows in the packet classes that need payload where payload is null. |
| `versions.distribution` | table | no | Count per protocol version. Warn for a version that is not in `ref.packet_names`. |

Session-level checks write `refined.sessions.verdict` (`accepted`, `flagged` or `quarantined`). Quarantined sessions go to `quality.quarantine_sessions` and are kept out of `refined.packets`.

**Consequences:** one bad session cannot block a day. A systematic defect (a schema change, an outage, a broken reference table) blocks the day. `quality.check_results` holds no session id and no player key (`subject = 'table'` for table checks). Session-level outcomes live in the sessions and quarantine tables, which erasure covers.

### 5. Silver: decoding, deduplication and packet names

**Options for deduplication**

1. Keep one row per (session_id, seq). If the content hash of the duplicates differs, quarantine the session. Chosen.
2. Keep the last delivered row. Rejected: delivery order is not stored, so "last" cannot be reproduced.
3. Deduplicate at read time. Rejected: each consumer would repeat it, and training would depend on it.

**Decision:**

- `refined.packets`: one row per (session_id, seq), with `direction`, `packet_type`, `packet_name` (joined from `ref.packet_names` on protocol_version, state, direction and wire_packet_id), `time_delta_ms` (to the previous packet of the same session and direction, from `offset_nanos`), `length`, `payload_hash` (xxHash64 over the payload), `protocol_version` (from `capture.raw`, since `capture.events` has no protocol column), and the decoded event fields listed in `dags/config/layers.yaml` (joined from `capture.events` on the same key). The table is partitioned by `date` and sorted by (session_id, seq).
- Hash-based conflict detection uses `xxhash64` over the content columns. Exact duplicates are dropped.
- Decoding: the configuration-phase packets (custom payload with the channel `minecraft:brand` or `minecraft:register`) are decoded in Python by a pure module `lake_decode` (VarInt length prefix, UTF-8 brand, NUL-separated channel list). The Spark job is still self-contained: the module is shipped to the driver in the same ConfigMap, as a Python file, not as the `lake` package. It is tested with plain bytes, no Spark.
- `refined.client_info`: one row per session, with brand, the channel list, the number of channels and the client version, from the configuration phase. Sessions without these packets get nulls and the verdict flag `no_config_phase`.
- `refined.plugin_messages`: one row per custom payload, with channel, direction, length and payload, for all sessions.
- `refined.sessions`: one row per session, with time bounds, packet counts, the quality verdict and flags.

**Consequences:** the payload is read only for the packet classes that need it, so the shuffle is over keys and hashes. The decoder covers only the configuration-phase formats, which are stable across 1.21.x to 26.3 (concept). Decoding more packets (chunks, entities) is later work.

### 6. Reference data: packet names and backend configuration

**Decision:**

- `ref.packet_names` (protocol_version, state, direction, packet_id, packet_name) and `ref.backend_config` (backend, period_start, period_end, anti_xray_mode) are loaded by the manual DAG `lake_reference`, from CSV files in the repository (`dags/config/reference/`). The files are reviewed in pull requests, and their Git history is the version history. The anti-xray mode file is transcribed by hand from the Paper configuration files (decided 2026-11-21, open question 13). A packet's backend is `capture.segments.backend_server`, joined on (session_id, segment).
- The loader validates every row (the state and direction vocabularies, unique keys) and replaces the table for the affected protocol versions in one `MERGE`. It does not call the network.
- The packet-name table is generated once from the PacketEvents registry for the supported range 1.21.x to 26.3 (concept, "Versionsbereich"). The generator is not part of this change. Its output is the input file, and the open question on PacketEvents coverage of 26.3 stays open (open question 5).

**Consequences:** a new protocol version is a reviewed file change, not a code change. A packet name is looked up with the version, never with the packet id alone, because the ids change between versions (concept).

### 7. Scheduling and triggers

**Decision:**

- `lake_refined` at `0 4 * * *` for day D-1. It declares the asset `iceberg://corvus/refined.packets`.
- `lake_labels` at `0 5 * * *`. It declares the asset `iceberg://corvus/labels.windows`.
- `lake_features` is scheduled on `Asset(refined.packets) & Asset(labels.windows)`. It builds features for the dates in the triggering events (`extra`).
- `lake_erasure` runs hourly and processes the pending requests in `quality.erasure_requests` (decision 14). Its manual trigger reruns one request. `lake_reference` is manual (`schedule=None`).
- Every Spark task of the lake DAGs uses the pool `lake_spark` (2 slots). The maintenance tasks use it too, so at most two lake SparkApplications run at once.

**Why not asset triggers from Corvus:** Corvus writes outside Airflow, so there is no asset event to subscribe to. A time-based first step and asset-aware steps after it are the pragmatic split. The chain is visible in the Airflow UI.

### 8. Idempotency, backfill and late data

**Decision:**

- A run for `(table, date)` overwrites exactly that partition (dynamic overwrite). Reruns give the same rows. `refined.sessions` and `features.windows` follow the same rule.
- Backfill takes a `dates` list in the run conf and uses the same code path. `max_active_runs=1` keeps backfills in order.
- Late data after the publish of its day stays out of silver until a manual backfill. The nightly run does not look back. The late-data rate is measured after the first weeks of data (open question 6).
- The `labels.windows` build recomputes the last 30 days every night, because a late admin label can change recent features. Older changes need a backfill.

### 9. Labels: sources and contract

Labels live in Iceberg tables in namespace `labels` and never in `capture`. Every change is an Iceberg snapshot, so a training run pins a snapshot id (decision 13).

**Sources** (concept, section "Labels selbst erfassen"):

1. Admins: `trusted` (known and unremarkable, a negative for "no hack") and `untrusted` (unknown; not a cheater label). `cheater` means a confirmed hack, with `hack_types` (for example killaura, fly, velocity).
2. Controlled recordings: the team plays on a test server with known hack clients and records when each module is on or off. These are the only timed labels for the hack type.
3. Blackhole: confirmed bans and staff decisions, as an additional source. Blackhole gives a broad time range. In v1 only a source adapter interface ships, with no concrete Blackhole source (open question 11).

**Contract, `labels.sessions`** (one row per admin or Blackhole decision):

- `label_id` (string, unique), `player_key`, `session_id` (nullable), `start_ts`, `end_ts` (timestamps, UTC)
- `label`: `trusted`, `untrusted`, `cheater`
- `hack_types` (array of string, empty unless `cheater`)
- `label_reason` (string from a fixed list such as `known`, `behaviour`, `client_not_vanilla`, `blackhole_ban`), kept separate from the label on purpose (concept: otherwise a model learns mods, not hacks)
- `source` (`admin`, `blackhole`), `editor` (the person's id or the Blackhole id), `edited_at`, `record_version`

**Contract, `labels.windows`** (controlled recordings):

- `label_id`, `session_id`, `player_key`, `module` (string), `active` (boolean, from the on and off events), `event_ts`, `source` (`controlled_recording`), `editor`, `record_version`
- A module-active window is the interval from an on event to the next off event, with the same module and session.

**Rules, pure and tested:**

- `untrusted` is unlabelled for training. It is never a negative and never a positive.
- `trusted` is a negative for the hack label in the overlapping session.
- `cheater` is a positive, and its `hack_types` are the classes of the multi-label task.
- Conflicting labels overlapping a session (for example `trusted` and `cheater`) give `unknown`, with a warning `labels.conflict` in `quality.check_results`. The conflict is never resolved silently.
- Upserts are `MERGE INTO` on `label_id`, keeping the newest `record_version`. Rows are never deleted except by erasure (decision 14).

### 10. How admins enter labels (decided, sub-points open)

**Options**

1. A CSV file per batch, validated and imported by a Spark task of `lake_labels`, reviewed in the repository or in Outline before the import. Recommended for v1: no new service, the import is idempotent, and every batch is a file with a history.
2. A small internal form that writes to the Polaris tables through an API. Rejected for v1: a new service with write rights to the labels, and a separate login path.
3. Superset with write access. Rejected: Trino and Superset are read-only by decision (decision 17). Superset stays the reading tool.

**Decision (2026-11-21, maintainer):** option 1, a CSV file per batch, reviewed, imported by `lake_labels`. The CSV schema is the `labels.sessions` contract, and the loader runs in the same `lake_labels` DAG as the Blackhole adapter. Still open (open question 2): who may import a batch, and where the CSV files are kept.

### 11. Train and test split by player

**Decision:** a split is assigned per player, never per window. The split value is a deterministic function of `player_key` and a fixed seed (`split_version`), written as a column `split` (`train` or `test`) in `features.windows`. Fixed proportions: 80 % train and 20 % test (open question 7 for the ratio). The assignment does not depend on the data, so a rerun gives the same split. A player in the test split never appears in train.

**Consequences:** a model cannot learn to recognise players by their windows (concept). Changing the seed or the ratio is a new `split_version` and a new dataset.

### 12. Gold: session statistics, token windows and versioning

**Decision:**

- `features.session_stats`: one row per session, with packets per second and per packet name, and the distribution of time deltas (percentiles), per direction.
- `features.windows`: fixed windows of 256 to 512 packets per session (concept: 3 to 13 seconds at this rate). A row has the window bounds, `player_key`, the `split`, the token array and `tokenizer_version`, the `label` (copied from `labels.sessions` or `labels.windows` for the window's time range), `source_snapshot_id` (the refined snapshot the window was built from) and `feature_version`.
- Tokens in v1: direction, the version-independent packet name (`packet_type`, Corvus' version-stable id, 0 when unmapped), a time-delta bucket, and a protocol-version token. This is the concept's starting mixture. Payload bytes are not tokenised in v1; they are for the later sequence model (concept, "Tokenisierung").
- Versioning: a change of the token rule, the window size or a feature definition creates a new `tokenizer_version` or `feature_version`. A built version is never rewritten, so a snapshot pinned by Corvus stays valid.
- `features.build_runs`: one row per (`feature_version`, `tokenizer_version`, `date`) build, with the refined snapshot id, the labels snapshot id, the code version and the build time.

**Concept mismatch noted:** the concept names the gold table `mc_gold.sequences` in one paragraph and `mc_gold.windows` in the table. This change uses `features.windows`, matching the table list.

**Handover to Corvus:** Corvus reads `features.windows` at a pinned snapshot id (`VERSION AS OF`), records the snapshot id, the `feature_version`, the session list and the `build_runs` row in its own dataset record, and creates an Iceberg tag at pin time (decision 13). Corvus' own feature computation moves here later (out of scope).

### 13. Training snapshots and erasure: Corvus owns the datasets, the lake guards

**Options**

1. Re-cut: after an erasure, create a new tag on the new head and keep the dataset id. The dataset looks the same, but its content has changed silently.
2. Corvus invalidates: Corvus drops the tags of the datasets that contain the player and marks those datasets `needs_rebuild` in its own dataset record. Corvus creates the replacement dataset later, with a new snapshot and id. Chosen.
3. The lake keeps its own invalidation table. Rejected: the dataset records belong to Corvus (decision 12), and a second record would split the truth.
4. Keep the tag and accept the erased rows in the snapshot. Rejected: it contradicts the erasure requirement.

**Decision:** option 2. Corvus runs its erasure of `capture` first (decision 14), so it knows which datasets contain the player and changes them in its own record. The lake does not hold dataset records.

**Guard in the lake:** before it deletes anything, `lake_erasure` checks that no tag of `features.windows` or `refined.packets` still contains the key. For each tag it runs `SELECT 1 FROM <table> VERSION AS OF '<tag>' WHERE player_key = '<key>' LIMIT 1`. A hit stops the request with status `failed_tagged_snapshot` and names the tag. Nothing is deleted. A missed tag is therefore never kept silently.

**Consequences:** a rebuilt dataset is a new dataset with a new snapshot and id. Corvus creates it and records the change. The lake only refuses to proceed while a stale tag exists.

### 14. GDPR erasure: Corvus erases `capture`, the lake erases downstream

**Ownership**

- Corvus deletes the player's rows in all five `capture` tables itself (`raw`, `events`, `sessions`, `segments`, `session_ends`) with copy-on-write deletes, tags the commits with `corvus.erasure-request`, and expires snapshots before the cut (Corvus PR `feat(services): erase and retain capture data in iceberg tables`). Corvus also drops the tags and marks the datasets of decision 13. What Corvus still has to add is the append of a pending row to `quality.erasure_requests` (task 2.7).
- The lake deletes the player's rows in every downstream table: `refined.packets`, `refined.client_info`, `refined.plugin_messages`, `refined.sessions`, `features.session_stats`, `features.windows`, `labels.sessions`, `labels.windows` and `quality.quarantine_sessions`. `quality.check_results` holds no keys (decision 4), so it is not touched.
- `lake_erasure` has no delete right on `capture`. It has read on the five `capture` tables, only for the guard below. `capture.segments` has no `player_key`, so its guard count joins through `capture.sessions` on `session_id`.

**Lake procedure, per request (idempotent):**

1. Read the requests with status `pending` from `quality.erasure_requests` (contract below).
2. Guard: count the player's rows in all five `capture` tables (`capture.segments` through `capture.sessions`). All counts must be 0. Otherwise the request gets status `failed_capture_not_empty` and nothing downstream is deleted. This shows that Corvus has not finished its part.
3. Guard: the tag check of decision 13.
4. Drop the audit branches of the downstream tables, then delete the player's rows in the downstream tables.
5. `expire_snapshots(older_than = now, retain_last = 1)` and `remove_orphan_files(older_than = now)` on each downstream table. Tags that were kept (datasets without the player) still protect their snapshots, as Iceberg intends.
6. Set the request to `done` with the lake run id, the completion time and the snapshot ids after cleanup.

**How the lake learns about an erasure (open question 14, decided 2026-11-21: option B):**

- Option A: Corvus calls the Airflow REST API to start `lake_erasure` with a run id equal to the request id. Immediate. But Corvus needs Airflow credentials and must know the DAG and its failure modes, and an Airflow outage forces retry logic into Corvus.
- Option B: Corvus appends a row to `quality.erasure_requests`, an Iceberg table in the lake, with its existing writer. `lake_erasure` runs every hour, processes the pending rows and writes the status back. Durable, auditable and visible in Superset. Costs up to one hour of latency and one table-level write grant for Corvus.
- Option C: the lake reads Corvus' erasure requests from a Corvus API or from Corvus Postgres. Keeps Corvus as the single source, but adds a network path and credentials from the lake to Corvus, and the lake depends on Corvus being up when it runs.

**Decision: option B** (maintainer, 2026-11-21). Hourly latency is far inside any erasure deadline, which is measured in weeks. The request is durable before the lake sees it, and the status comes back to the same table, so Corvus can confirm completion without polling the lake. It needs no Airflow credentials in Corvus and no new API.

`quality.erasure_requests` is a lake table. Corvus' Postgres table `corvus.erasure_requests` stays the Corvus-side state.

**Contract of `quality.erasure_requests`:** `request_id` (string, unique), `player_key`, `requested_at`, `capture_erased_at`, `capture_raw_snapshot_id`, `capture_events_snapshot_id` (written by Corvus after its erasure), and `status` (`pending`, `done`, `failed_capture_not_empty`, `failed_tagged_snapshot`), `failure_reason`, `lake_run_id`, `completed_at` (written by the lake). Corvus has table-level write on this table only.

**Ordering consequence:** capture is erased before the downstream tables. If ingest writes the player's rows again in between, the next nightly build rebuilds those days from `capture` and the rows return. Corvus must therefore stop ingesting the erased key (open question 15).

**Consequences:** the physical deletion is immediate in both places, not after a retention window. The cost is that expiry is aggressive for the affected tables, with no time travel back to before the erasure. That is the intended effect. The request rows hold the key as evidence of the erasure, which is a retention question (open question 16).

### 15. Retention

**Decision:**

- `capture.raw`: permanent, `retention_days: null`. The maintainer's decision. Reviewed yearly, as the concept's Datenschutz section requires (open question 3 for the purpose and the review date).
- `capture.events`: as today (no retention). Read by the silver build (decision 2). Erased by Corvus on request.
- `capture.sessions`, `capture.segments`, `capture.session_ends`: no retention set by this change. Read by the silver build (decision 2). Erased by Corvus on request.
- `refined.*`, `features.*`: rebuildable from `capture.raw`, so the default proposal is 365 days, applied by the maintenance DAG with the day column per table. Open question 3.
- `labels.*`: no automatic expiry. Labels are reviewed yearly with the data.
- `quality.check_results`: 400 days. `quality.quarantine_sessions`: 30 days (it holds session ids and keys). `quality.erasure_requests`: kept as evidence of each erasure. Its key retention is open (open question 16).

### 16. Spark sizing

**Decision:** the sizing is set once, without a measurement gate (the maintainer dropped the 100-player gate). Driver 1 core and 2 GiB. Two executors of 2 cores and 4 GiB each, with dynamic allocation between 1 and 2. `spark.sql.shuffle.partitions` is 32. The job reads only the columns it needs, and payload is read only for the packet classes that need it (decision 5), so the shuffle is over keys and hashes, not over the full 15 GB.

**Monitoring instead of a gate:** the run duration and the peak memory are recorded in the runbook after the first weeks (task 9.2). A run longer than one hour, or any failure caused by memory, is the trigger to raise the sizing.

### 17. Trino is read-only (deviation)

**Decision:** Trino is the read path for staff (`lake_sql`, Superset). It does not write silver, gold, labels or maintenance. The concept gives Trino the write role for silver and gold, and the maintenance tasks.

**Why it deviates:** writes through Trino would bypass the audit gate and the idempotent partition overwrite, and Superset users would have write rights in the same engine. Spark jobs from Airflow write, Spark maintenance (already deployed) compacts and expires, and Trino reads. Compaction through Trino would also need the same delete rights that `lake_erasure` holds, which we keep separate.

### 18. Testing

**Decision:**

- Pure code (config parsing, checks, the decoder, the silver builders, the label rules, the split, the token windows, erasure statements, tag checks) is unit-tested with no cluster, no Spark and no clock. Time is passed in as a `datetime`.
- The decoder is tested with byte fixtures, including the VarInt edges and a channel list with several entries.
- Assertions on SQL check its structure (dedup by row_number over the key, no statement writes to `capture`, every erasure delete filters by `player_key`). They do not compare whole strings.
- DAG import tests run in the existing `test_dag_import.py` (marker `airflow`, CI only).
- One integration test runs the write, check and publish statements on a local Spark session with a file-based Iceberg catalog in a temp directory (marker `spark`, CI only). It covers the WAP outcome, the rerun, and an erasure for one key on a small fixture.
- No `sleep`, no wall-clock reads and no shared state between tests.

## Open questions

Items marked "Decided" were decided by the maintainer on 2026-11-21. Everything else stays open and is not decided silently.

1. **Corvus schema of `capture.raw`. Resolved 2026-11-21** from Corvus origin/main (see "Verified facts from Corvus origin/main"). The key is (session_id, seq), `offset_nanos` counts from session start, `packet_type` is the version-stable id and `wire_packet_id` the wire id. The backend is in `capture.segments`, and dropped-packet counters are in `capture.session_ends`. Task 1.1 keeps the re-check on the deployed table.
2. **Label entry path. Partly decided.** Decided 2026-11-21: option 1, a CSV file per batch, reviewed, imported by `lake_labels` (decision 10). Still open: who may import a batch, and where the CSV files are kept.
3. **Retention and purpose.** The default proposals in decision 15 need a maintainer decision, together with a written purpose and review date for permanent bronze.
4. **Columns of `capture.events` in silver.** The layers read `capture.events` (decision 2). Still open: which of its columns `refined.packets` carries (task 1.1 and `layers.yaml`).
5. **Packet-name coverage.** Whether PacketEvents already knows protocol 26.3 (concept, Versionsbereich) and the generator for `ref.packet_names`.
6. **Late-data rate.** Unknown until the pilot runs. It sets whether the nightly run needs a lookback.
7. **Split ratio.** 80/20 by player is a default. Small test sets with few players are noisy; the ratio is set after the first label counts.
8. **Models and retention.** Whether models trained on a dataset marked `needs_rebuild` need a retention exception, or are retrained. This is Corvus' decision, recorded here because erasure changes the dataset.
9. **Plaintext chat and login names.** The maintainer decided plaintext in bronze. The concept recommends `meta_only` for text. The layers keep text out of silver and gold, but the bronze risk needs a privacy review and a written purpose before go-live (task 1.4).
10. **Airflow version. Resolved 2026-11-21.** `pyproject.toml` on origin/main pins `apache-airflow==3.4.0`, and Kubernetes-FLUX main sets `airflowVersion` 3.4.0. Both agree, so the earlier 3.3.2 and 3.2.2 pins are superseded. Task 1.2 keeps the cluster check and the pool.
11. **Blackhole source and key mapping. Stays open on purpose** (decided 2026-11-21). `lake_labels` ships a source adapter interface with no concrete Blackhole source in v1. Still open: the access path (API export, database read or pushed events), and how Blackhole gets the same `player_key` as Corvus, which uses an HMAC key that the lake does not hold. The label contract is fixed here; the source is not.
12. **Protocol coverage.** Whether the configuration-phase decoder holds across the whole range 1.21.x to 26.3 (the concept says so for 1.21.x, which needs a check on real data), before the brand and channel features are trusted.
13. **Anti-xray mode source. Decided 2026-11-21:** a reviewed CSV file in `dags/config/reference/`, transcribed by hand from the Paper configuration files and loaded by `lake_reference` (decision 6). The concept asks for the mode per backend and period; the periods are part of the file.
14. **Erasure trigger. Decided 2026-11-21: option B.** Corvus appends to `quality.erasure_requests` and `lake_erasure` polls hourly (decision 14). The Corvus change is task 2.7. Blackhole-originated requests, if any, enter the same table.
15. **Ingest after erasure.** Still open. Known Corvus gap: ingest checks `players.erased_at` but never reads `corvus.blocklist`, so a pseudonym erased before it was ever seen is not blocked at ingest. Corvus' design decides how this is closed (for example a block list checked at ingest). It is required before go-live.
16. **Retention of erasure evidence.** `quality.erasure_requests` holds the key as proof of erasure. Whether the key stays, is hashed, or is removed after a set period is a decision for the maintainers together with the legal basis.
17. **Seq gaps and payload-less frames (new).** `capture.raw` keeps only frames with a payload. If Corvus sees frames without payload, their `seq` values are missing from `capture.raw`, and `seq.gaps` (decision 4) would report them as dropped frames. Confirm with Corvus whether such frames exist, and check real data, before the gap threshold is trusted.
18. **Wall-clock source for `time.skew` (new).** `offset_nanos` counts from session start, so the check needs a wall-clock time on both sides. `capture.sessions.started_at` and `capture.segments.started_at` exist, but whether they give a server and client pair per packet is not verified. Decide the source before the check is written.

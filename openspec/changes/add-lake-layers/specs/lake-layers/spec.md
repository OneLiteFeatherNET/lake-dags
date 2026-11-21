## ADDED Requirements

### Requirement: Layers write only their own namespaces

THE lake jobs SHALL write the tables of the namespaces `refined`, `features`, `labels`, `ref` and `quality` of catalog `corvus`. THE lake jobs SHALL NOT write into `capture` and SHALL NOT delete rows in `capture`.

#### Scenario: Generated writes stay in layer namespaces

- **GIVEN** the statement builders of all lake jobs
- **WHEN** the statements are generated for one day
- **THEN** every write target starts with `refined.`, `features.`, `labels.`, `ref.` or `quality.`
- **AND** no write statement targets `capture.`

#### Scenario: Capture is read only for the lake

- **GIVEN** the statement builders of all lake jobs
- **WHEN** the statements that reference `capture.` are listed
- **THEN** every such statement is a read (a select)

### Requirement: Silver is built from the Corvus capture tables

THE refined build SHALL read `capture.raw` for packet identity and payload, and `capture.events` for the decoded event fields, joined on (session_id, seq). THE refined build SHALL read `capture.sessions`, `capture.segments` and `capture.session_ends` for session facts, and SHALL take one row per `session_id` (and `segment`) from them, because redelivery appends duplicates. THE refined build SHALL read no other source table.

#### Scenario: Source tables of the refined build

- **GIVEN** the statements of the refined build for one day
- **WHEN** their source tables are listed
- **THEN** the source tables are `capture.raw`, `capture.events`, `capture.sessions`, `capture.segments` and `capture.session_ends` only
- **AND** the packet join key is (session_id, seq)

#### Scenario: Redelivered session row is read once

- **GIVEN** two rows of `capture.sessions` for session `s1`, one of them a redelivery
- **WHEN** the refined build runs
- **THEN** `refined.sessions` holds exactly one row for `s1`

### Requirement: Duplicates are removed by session and sequence

WHEN `capture.raw` contains more than one row for the same (session_id, seq), THE refined build SHALL keep exactly one row for that key in `refined.packets`.

#### Scenario: Exact duplicate is dropped

- **GIVEN** two identical rows for (session `s1`, seq `7`) in `capture.raw` for day D
- **WHEN** the refined build for day D runs and passes its checks
- **THEN** `refined.packets` contains exactly one row for (`s1`, `7`)
- **AND** the duplicate is counted in the `dup.rate` result

#### Scenario: Conflicting duplicate quarantines the session

- **GIVEN** two rows for (session `s2`, seq `3`) with different content hashes
- **WHEN** the refined build for day D runs
- **THEN** no row of session `s2` is written to `refined.packets`
- **AND** session `s2` is written to `quality.quarantine_sessions` with check id `dup.conflict`

### Requirement: Text payloads are not copied into silver or gold

THE refined and features tables SHALL NOT contain the payload of chat, command, login, sign or book packets. THE table `refined.packets` SHALL NOT have a payload column.

#### Scenario: Packet table has no payload column

- **GIVEN** the table `refined.packets` after the refined build
- **WHEN** its columns are read
- **THEN** no column is named `payload`

#### Scenario: Chat payload is not in plugin messages

- **GIVEN** a chat packet in `capture.raw`
- **WHEN** the refined build runs
- **THEN** `refined.plugin_messages` holds no row for that packet

### Requirement: Client information comes from the configuration phase

THE refined build SHALL write one row per session to `refined.client_info`, holding the brand, the channel list, the number of channels and the client version, decoded from the configuration-phase packets of that session.

#### Scenario: Brand and channels are decoded

- **GIVEN** a session whose configuration phase sent the brand `fabric` and the channels `minecraft:brand` and `fabric:networking`
- **WHEN** the refined build runs
- **THEN** `refined.client_info` holds brand `fabric`, two channels and both channel names

#### Scenario: Session without configuration packets

- **GIVEN** a session with no configuration-phase packets
- **WHEN** the refined build runs
- **THEN** `refined.client_info` holds a row for that session with null brand and channels
- **AND** `refined.sessions` flags `no_config_phase` for that session

### Requirement: Packet names depend on the protocol version

THE refined build SHALL set `packet_name` by joining `ref.packet_names` on protocol version, state, direction and wire packet id, and SHALL NOT join on the packet id alone.

#### Scenario: Same packet id in two versions

- **GIVEN** packet id `35` in state play and direction S2C has different names in protocol versions `767` and `769`
- **WHEN** rows of both versions are refined
- **THEN** each row carries the name of its own protocol version

### Requirement: Publication only through a passing audit branch

THE refined build SHALL write each day to an audit branch, and SHALL fast-forward `main` to that branch only when no blocking check has failed. WHEN a blocking check fails, THE refined build SHALL leave `main` unchanged.

#### Scenario: Passing day is published

- **GIVEN** the audit branch for day D passes all blocking checks
- **WHEN** the publish step runs
- **THEN** `main` of `refined.packets` points to the audit branch snapshot
- **AND** the audit branch is removed

#### Scenario: Failing day is not published

- **GIVEN** the audit branch for day D fails the blocking check `schema.capture_raw`
- **WHEN** the publish step runs
- **THEN** `main` of `refined.packets` is unchanged
- **AND** the audit branch is removed
- **AND** `quality.check_results` holds verdict `fail` for `schema.capture_raw`

#### Scenario: Forced publish is recorded

- **GIVEN** the audit branch for day D fails `rows.baseline`
- **AND** the run conf sets `force_publish` to true
- **WHEN** the publish step runs
- **THEN** `main` points to the audit branch snapshot
- **AND** `quality.check_results` holds verdict `overridden` for `rows.baseline`

### Requirement: Non-blocking checks are recorded, not enforced

THE refined build SHALL record every check result in `quality.check_results`, and SHALL NOT let a non-blocking failure prevent publication.

#### Scenario: Warning does not block

- **GIVEN** the duplicate rate of day D is 6 %, above the 5 % threshold
- **WHEN** all blocking checks pass
- **THEN** `main` points to the audit branch snapshot
- **AND** `quality.check_results` holds verdict `warn` for `dup.rate`

#### Scenario: Missing drop counter is not a failure

- **GIVEN** session `s11` has no row in `capture.session_ends` (a crashed session)
- **WHEN** the refined build runs
- **THEN** `refined.sessions` holds `not_available` for `drops.reported` of `s11`
- **AND** `main` is published if the blocking checks pass

#### Scenario: Dropped counters are summed per session

- **GIVEN** two rows of `capture.session_ends` for session `s12` with `dropped_queue_full` 3 and 2
- **WHEN** the refined build runs
- **THEN** `refined.sessions` holds a dropped total of 5 for `s12`

### Requirement: Sequence gaps are checked per session

THE refined build SHALL compute the missing-sequence ratio for each session over the `seq` values of both directions, and SHALL quarantine a session whose ratio is above 1 %.

#### Scenario: Gap ratio above threshold

- **GIVEN** session `s3` has a gap ratio of 2 %
- **WHEN** the refined build runs
- **THEN** `refined.sessions` holds verdict `quarantined` for `s3`
- **AND** no row of `s3` is in `refined.packets`

#### Scenario: Incomplete session is flagged only

- **GIVEN** session `s4` has no disconnect event
- **WHEN** the refined build runs
- **THEN** `refined.sessions` holds verdict `flagged` for `s4`
- **AND** the rows of `s4` are in `refined.packets`

### Requirement: Row counts reconcile with capture.raw

THE refined build SHALL verify, as the blocking check `rows.reconcile`, that the distinct (session_id, seq) count of the day in `capture.raw` equals the rows in `refined.packets` plus the `row_count` of the quarantined sessions of that day.

#### Scenario: Silent loss is blocked

- **GIVEN** a build that drops 10 rows without quarantining their session
- **WHEN** the check `rows.reconcile` runs
- **THEN** the verdict is `fail` and `main` is unchanged

### Requirement: Builds are idempotent per day

WHEN the refined build for day D runs again, THE build SHALL overwrite only partition `date = D` of its tables, and SHALL leave every other partition unchanged.

#### Scenario: Rerun gives the same rows

- **GIVEN** day D was built and published
- **WHEN** the refined build for day D runs again with unchanged input
- **THEN** the rows of day D are identical to the first run
- **AND** the rows of every other day are unchanged

### Requirement: Reference loads validate and replace

THE reference loader SHALL read the reviewed CSV files in `dags/config/reference/` (packet names, and backend configuration with the anti-xray mode), SHALL reject a file with an unknown state or direction, or with duplicate keys, and SHALL replace the rows of the affected protocol versions in one `MERGE`.

#### Scenario: Invalid reference file is rejected

- **GIVEN** a packet-name file with state `playing`
- **WHEN** the loader runs
- **THEN** the run fails before any write
- **AND** the table `ref.packet_names` is unchanged

### Requirement: Labels are Iceberg tables with a fixed contract

THE label import SHALL read admin label batches from CSV files and the controlled recordings, SHALL write them into `labels.sessions` and `labels.windows` only, with the fields of design decision 9, and SHALL upsert by `label_id`, keeping the newest `record_version`. Blackhole labels SHALL enter through the source adapter interface, which has no concrete source in v1.

#### Scenario: Newer version replaces older

- **GIVEN** `labels.sessions` holds label `L1` at `record_version` 1
- **WHEN** the import delivers `L1` at `record_version` 2
- **THEN** the table holds one row for `L1` with `record_version` 2

#### Scenario: Same record again changes nothing

- **GIVEN** `labels.sessions` holds `L1` at `record_version` 2
- **WHEN** the import delivers `L1` at `record_version` 2 again
- **THEN** the table holds one row for `L1` with unchanged values

### Requirement: Untrusted is unlabelled for training

THE feature build SHALL assign label `unknown` to windows of a session labelled `untrusted`, and SHALL NOT use `untrusted` as a negative or a positive.

#### Scenario: Untrusted session

- **GIVEN** session `s5` is labelled `untrusted` by an admin
- **WHEN** the feature build runs
- **THEN** every window of `s5` has label `unknown`

#### Scenario: Trusted session is a negative

- **GIVEN** session `s6` is labelled `trusted` with an empty hack type list
- **WHEN** the feature build runs
- **THEN** every window of `s6` has label `trusted`

#### Scenario: Cheater session carries hack types

- **GIVEN** session `s7` is labelled `cheater` with hack types `killaura` and `velocity`
- **WHEN** the feature build runs
- **THEN** every window of `s7` has label `cheater` and carries both hack types

### Requirement: Conflicting labels are not resolved silently

WHEN a `trusted` and a `cheater` label overlap the same session, THE label build SHALL give the session label `unknown` and SHALL record verdict `warn` for check `labels.conflict`.

#### Scenario: Overlapping labels conflict

- **GIVEN** a `trusted` and a `cheater` label both overlap session `s8`
- **WHEN** the label build runs
- **THEN** the windows of `s8` have label `unknown`
- **AND** `quality.check_results` holds verdict `warn` for `labels.conflict`

### Requirement: Controlled recordings give module windows

THE label build SHALL derive a module-active window from each on event and the next off event of the same module and session in `labels.windows`.

#### Scenario: Module on and off

- **GIVEN** the events "killaura on" at 10:00:00 and "killaura off" at 10:00:30 in session `s9`
- **WHEN** the label build runs
- **THEN** a window for `killaura` in `s9` spans 10:00:00 to 10:00:30

### Requirement: Train and test split by player

THE feature build SHALL assign `split` per `player_key` from a deterministic function of the key and the split seed, and SHALL NOT place a player in both splits.

#### Scenario: Stable split

- **GIVEN** the same player key and seed
- **WHEN** the split is assigned on two runs
- **THEN** both runs give the same value

#### Scenario: No player in both splits

- **GIVEN** a feature build for a set of players
- **WHEN** the split of each window is listed
- **THEN** no `player_key` has both `train` and `test`

### Requirement: Feature rows record their sources

EVERY row of `features.windows` SHALL hold `tokenizer_version`, `feature_version` and `source_snapshot_id`, and THE build SHALL write one row per (`feature_version`, `tokenizer_version`, `date`) to `features.build_runs`.

#### Scenario: Provenance after a build

- **GIVEN** the build for `v1`, tokenizer `t1` and day D finished
- **WHEN** `features.build_runs` is queried for that key
- **THEN** exactly one row exists and it holds the refined snapshot id that the build read

### Requirement: Built versions are not rewritten

WHEN a token rule, window size or feature definition changes, THE build SHALL write a new `tokenizer_version` or `feature_version`, and SHALL NOT change rows of a version that was already built.

#### Scenario: New tokenizer leaves old rows

- **GIVEN** `features.windows` holds rows of tokenizer `t1` for day D
- **WHEN** tokenizer `t2` is built for day D
- **THEN** the rows of `t1` for day D are unchanged

### Requirement: Erasure removes one player from every downstream table

WHEN the lake erasure processes a pending request for a `player_key`, THE erasure SHALL delete the rows of that key in `refined.packets`, `refined.client_info`, `refined.plugin_messages`, `refined.sessions`, `features.session_stats`, `features.windows`, `labels.sessions`, `labels.windows` and `quality.quarantine_sessions`.

#### Scenario: Key is gone from every downstream table

- **GIVEN** a key that has rows in every listed downstream table and no rows in `capture`
- **WHEN** the lake erasure completes for that key
- **THEN** a query for that key returns zero rows in each listed table
- **AND** the request has status `done`

#### Scenario: Erasure is idempotent

- **GIVEN** the lake erasure already completed for the key
- **WHEN** it runs again for the key
- **THEN** it deletes no rows and the request stays `done`

### Requirement: Erasure expires downstream snapshots immediately

AFTER the deletes, THE lake erasure SHALL drop the audit branches of the affected downstream tables, then run `expire_snapshots` and `remove_orphan_files`, each with `older_than` set to the run time, on each affected downstream table.

#### Scenario: Old downstream snapshots are gone

- **GIVEN** the lake erasure completed for a key
- **WHEN** the snapshots of an affected downstream table are listed
- **THEN** no snapshot older than the run time remains, except snapshots held by tags

### Requirement: No lake principal deletes from capture

THE principals `lake_pipeline` and `lake_erasure` SHALL NOT hold delete rights on `capture`. Corvus SHALL erase its own `capture` tables.

#### Scenario: Delete on capture is refused for the lake

- **GIVEN** a request by `lake_erasure` to delete from `capture.raw`
- **WHEN** the request reaches Polaris
- **THEN** the response is HTTP 403

### Requirement: Erasure stops while a tag still holds the player

WHEN a tag of `features.windows` or `refined.packets` still contains rows of the requested key, THE lake erasure SHALL delete nothing, SHALL set the request to status `failed_tagged_snapshot`, and SHALL name the tag in `failure_reason`.

#### Scenario: Tagged dataset still contains the key

- **GIVEN** tag `dataset_1` on `features.windows` still contains rows of key `pc`
- **WHEN** the lake erasure runs for `pc`
- **THEN** no downstream row of `pc` is deleted
- **AND** the request has status `failed_tagged_snapshot` with `dataset_1` in `failure_reason`

#### Scenario: Tagged dataset without the key is kept

- **GIVEN** tag `dataset_2` on `features.windows` contains no rows of key `pc`
- **WHEN** the lake erasure runs for `pc`
- **THEN** the tag `dataset_2` still exists

### Requirement: Permanent raw retention

THE maintenance configuration SHALL keep `capture.raw` permanently, with `retention_days` null, and SHALL apply a retention to each other new table as listed in design decision 15.

#### Scenario: Config keeps raw

- **GIVEN** `dags/config/maintenance.yaml`
- **WHEN** the entry for `capture.raw` is read
- **THEN** `retention_days` is null

### Requirement: Quality results hold no keys

THE table `quality.check_results` SHALL NOT contain a `player_key` or a `session_id`.

#### Scenario: Session check writes no session id

- **GIVEN** session `s10` was quarantined by `seq.gaps`
- **WHEN** `quality.check_results` is queried for `s10`
- **THEN** zero rows are returned

### Requirement: People have read-only SQL

THE principal `lake_sql` SHALL have read rights only on the namespaces of this change, and SHALL NOT have write rights on any of them.

#### Scenario: Staff query is read only

- **GIVEN** a Trino query by a `lake_sql` user that inserts into `refined.packets`
- **WHEN** the query reaches Polaris
- **THEN** the write is refused

### Requirement: Spark tasks share one pool

EVERY Spark task of the lake DAGs, including the maintenance tasks, SHALL declare `pool="lake_spark"`.

#### Scenario: Pool on all Spark tasks

- **GIVEN** the DAG definitions of this repository
- **WHEN** the tasks that create a SparkApplication are listed
- **THEN** each declares `pool="lake_spark"`

### Requirement: Pure logic is tested without a cluster

THE checks, the decoder, the silver and label rules, the split, the token windows and the erasure statements SHALL be tested without Spark, Airflow, Kubernetes or network access.

#### Scenario: Fast suite needs no services

- **GIVEN** an environment without Spark, Airflow or cluster access
- **WHEN** the tests excluding the markers `airflow` and `spark` run
- **THEN** all tests pass and none reads the system clock

### Requirement: Erasure requests come from Corvus and are processed hourly

THE lake erasure SHALL process every request with status `pending` in `quality.erasure_requests`, SHALL run hourly, and SHALL write back the status `done` or a failure status with the lake run id.

#### Scenario: Pending request is processed

- **GIVEN** Corvus wrote a pending request for key `pd` after erasing `capture`
- **WHEN** the hourly erasure run starts
- **THEN** the request for `pd` ends with status `done` and a `lake_run_id`

#### Scenario: Capture not yet empty

- **GIVEN** a pending request for key `pe` while any of the five `capture` tables still holds rows of `pe`
- **WHEN** the hourly erasure run starts
- **THEN** no downstream row of `pe` is deleted
- **AND** the request has status `failed_capture_not_empty`

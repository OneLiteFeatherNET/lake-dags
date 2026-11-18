## Why

Corvus now writes `capture.raw`: the complete packet capture of both directions, stored in plaintext in Iceberg and kept permanently. It includes chat and login names, with a pseudonymous `player_key`. The raw table is the record of what players sent and received, but nothing in the lake turns it into analysis-ready data. Without layers, every analysis decodes the packets again, data quality is unknown, and there are no labels and no table ready for model training.

The volume is small: at most 10 players per day, 1 to 4 hours each, which is 1.4 to 14 million packets and 1 to 15 GB per day. One nightly Spark job per layer is enough.

Admins label players as trusted, untrusted or cheater, and the team records controlled sessions on a test server with known hack clients. Neither source exists in the lake yet. Blackhole bans and reports are an additional source.

## What Changes

The code ships as `feat(dags)`, PR title `feat(dags): add refined, label and feature layers`. All code changes are in `lake-dags`. Platform changes are listed under Dependencies and live in Kubernetes-FLUX.

- Namespaces in catalog `corvus`. `capture` (bronze, owned by Corvus) is kept. New: `refined` (silver), `features` (gold), `labels`, `ref` (reference data) and `quality`. Each maps to a concept namespace (`mc_silver` to `refined`, and so on). design.md, decision 1, explains why the deployed names are kept.
- Silver, built from `capture.raw` joined with the decoded fields of `capture.events`: `refined.packets` (deduplicated on (session_id, direction, seq), with packet name and time delta), `refined.client_info` (per session: brand, channel list, client version from the configuration phase), `refined.plugin_messages` (custom payloads with payload kept), `refined.sessions` (per-session summary and quality verdict).
- Gold: `features.session_stats` (rates per packet name, gap distribution), `features.windows` (token windows with `tokenizer_version` and the source snapshot id), `features.build_runs` (provenance).
- Labels: `labels.sessions` (trusted, untrusted or cheater, hack types, `label_reason`, source, editor, time range) and `labels.windows` (module on and off times from controlled recordings). Labels are Iceberg tables, versioned by snapshot. Training uses a train or test split by player.
- Reference: `ref.packet_names` (protocol version, state, direction, packet id to name) and `ref.backend_config` (anti-xray mode per backend and period). Both are loaded from reviewed files in the repository.
- Write-Audit-Publish for `refined`, as before. Data-quality checks now include the seq-gap check per session and direction and a dropped-packets signal when Corvus reports one.
- DAGs: `lake_reference` (manual), `lake_refined` (daily), `lake_labels` (daily), `lake_features` (triggered by the assets of `lake_refined` and `lake_labels`), `lake_erasure` (hourly, processes the erasure requests Corvus writes to `quality.erasure_requests`, and deletes the player in every downstream layer).
- `dags/config/maintenance.yaml` changes `capture.raw` retention from 21 days to permanent (`retention_days: null`), as decided by the maintainer. This is a change in deployed behaviour and is listed in the tasks.

## Deviations from the concept document

- Trino is read-only for people (Superset and staff). The concept has Trino writing silver and gold. Writes are Spark jobs started by Airflow, so that every write goes through reviewed code, the audit gate and idempotent partition overwrites.
- Namespace names are the deployed `capture` plus `refined`, `features`, `labels`, `ref` and `quality`, not `mc_*`. The concept's `mc_*` names would need a second catalog or a rename of the Corvus namespace.
- The concept's privacy section recommends `meta_only` for chat and login names. The maintainer has decided that all packets are stored in plaintext. This change does not copy chat or login payloads into silver or gold. It requires a privacy review before go-live (task 1.4).

## Dependencies (Kubernetes-FLUX, separate change, `feat(data)`)

None of this is implemented here, and the code cannot run in production until it exists.

- Polaris namespaces `refined`, `labels`, `features`, `ref` and `quality` in catalog `corvus`.
- Polaris principal role `lake_pipeline`: table-level read on `capture.raw` and `capture.events`; create and write on `refined`, `features`, `labels`, `ref` and `quality`. No delete on any table in `capture`.
- Polaris principal role `lake_erasure`, separate from the pipeline: delete on the downstream tables (`refined`, `features`, `labels`, `quality` quarantine); read on `capture.raw` and `capture.events` for the guard; write on the status of `quality.erasure_requests`; the maintenance grants for expire and orphan cleanup on the downstream namespaces. No delete on `capture`.
- Corvus (Kubernetes-FLUX and Corvus PR `feat(services): erase and retain capture data in iceberg tables`): table-level write on `quality.erasure_requests` only, for the principal Corvus already uses for ingest. Corvus erases its own `capture` tables.
- Polaris maintenance grants (`lake_maintenance`) and read grants (`lake_sql`) extended to the new namespaces.
- Entra ID: app roles `lake-pipeline` and `lake-erasure` on the API app "Polaris", client apps assigned to them, client secrets as SOPS Secrets.
- Airflow pool `lake_spark` (2 slots), used by all Spark tasks of this repository.

## Out of scope

- Model training, the ONNX export and the scorer (Corvus).
- Phase 2 and later of the concept: `refined.world_targets`, `refined.player_track`, `features.world_features` (world and line-of-sight analysis). The namespace names are reserved; the tables are not built here.
- Streaming (Flink, NATS consumers). Batch only.
- Sampling. Phase 1 keeps every packet, as the concept recommends.
- Changes to the Corvus writer, the Minecraft plugin and Blackhole. Blackhole's access path is an open question.

## Impact

- Existing tables are only read, except `capture.raw` retention, which becomes permanent and needs the yearly review the concept asks for.
- New tables are additive. Existing DAG ids are unchanged. The maintenance DAG gains a pool and new tables.
- Erasure removes one player from every downstream layer. Corvus erases `capture`. The procedure and the trigger are in design, decision 14.

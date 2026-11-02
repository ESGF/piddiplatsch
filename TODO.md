# TODO

## Agreed catalog and work-stream design

See [the design agreed on 2026-11-01](docs/work-stream-design.md) for the implemented
single `piddi.db` with `catalog_items`, `work_items`, `kafka_checkpoints`, and
`stats`. It records the exact columns, snapshot/state rules, JSONL output,
archive replay requirements, and retention decisions. This format requires a fresh start; no legacy migration is provided.

- [x] Implement the four-table schema and transactional harvest handoff for a
  fresh start; backward compatibility and migration are intentionally omitted.
- [x] Make mapping/publication use saved work snapshots and processing states;
  keep generated Handles in JSONL batches linked by `batch_id`.
- [x] Add sequential round robin scheduling of project batches, initially capped
  at 2,000 work entries each. Separate selection, batch execution, and scheduling
  to allow a bounded project-worker pool later; defer parallel execution and
  worker claims until durable recovery and ordering are established.
- [x] Add retained batch summaries and configurable cleanup of old published work without
  double counting results. Cleanup stays disabled by default; keep all work for
  now and invalid work available for explicit retry.
- [x] Keep `VACUUM` an explicit supervised operation with Piddi stopped; scheduled
  maintenance performs configured logical cleanup without physical compaction.

## Handle service

- [ ] Test REST publication against a disposable real Handle prefix.
- [ ] Confirm authentication, HTTPS/CA requirements, error handling, and safe
  retry behavior without exposing secrets.
- [ ] Document safe REST publication concurrency and rate limits.
- [ ] Investigate server-side batching only if the deployed service documents a
  multi-Handle endpoint with per-item results.

## Publication spool

- [x] Compress completed batch output and closed receipts; offer optional
  age-based removal, disabled by default, independently of DB work retention.
- [ ] Extend durability rules and storage policies based on production measurements.
- [ ] Add backlog metrics, publication rate limiting, and optional watch mode.
- [ ] Add an outbox validation command that never publishes.
- [ ] Define multi-target routing and track delivery independently per target.

## Recovery and Kafka

- [x] Archive each Kafka event to JSONL before further processing, including
  topic, partition, offset, key, and the original message payload. Keep archived
  events unchanged regardless of processing outcome and preserve ordering
  within each partition.
- [x] Rotate dump files by a configurable event count as well as by day. The
  initial harvest can contain the complete retained history and produce several
  GB in one day. Split a day's archive into numbered parts with deterministic
  replay ordering; preserve every event across rotation and restart.
- [x] Support Kafka and archived JSONL files as alternative inputs to the same
  processing flow, including rebuilding the STAC catalog after Kafka retention
  has removed old events. Dumps are an archive, not the batch pending queue.
  Replay requires the new coordinate-bearing archive envelope.
- [ ] Keep event error policy separate from batching: either stop without
  acknowledging an invalid event, or optionally record the failure, acknowledge
  it, and continue. Never modify an original event; a later correction on Kafka
  is a new event.
- [ ] Define and test Kafka acknowledgement behavior across persistence errors,
  automatic commits, transient failures, and restarts.

## Harvest catalog cache and mapping

`harvest` reconstructs a persistent local STAC catalog from Kafka
before mapping. `map` reads the latest complete state of each versioned item from
that catalog, with no remote STAC dependency. Keep older dataset versions for
local predecessor lookup, but overwrite successive updates to the same item.

- [ ] Document ordering assumptions for items published across multiple Kafka
  partitions or topics, including shared-project checkpoint durability.
- [ ] Define catalog compaction and journal growth controls and minimize redundant
  indexes and checkpoint metadata.
- [ ] Defer alternative database backends and the older per-project database
  proposal until after the agreed single-SQLite work-stream design is evaluated.
- [ ] Define and add force-refresh semantics that never blindly reapply already
  processed, potentially non-idempotent patches. Add completeness reporting
  beyond the existing per-item remote current-state recovery.
- [ ] Add project-scoped cache inspection/removal with safe database closure and
  cleanup of associated journal files. Report item counts, unresolved records,
  checkpoints, and disk usage.
- [x] Remove retained legacy retry library helpers.
- [ ] Add an explicit harvest force option with diagnosed skip/checkpoint semantics
  for unresolved events; default harvest remains fail-fast.
- [ ] Add a configurable publication failure limit aligned with mapping.
- [x] Remove direct remote PATCH mapping, transient configuration, and the
  legacy force bypass.
- [ ] Run shared backend correctness tests for publications, consecutive patches,
  duplicate replay, interrupted transactions, missing bases, project isolation,
  predecessor selection, clean/force behavior, and independent project rebuilds.
- [ ] Benchmark the agreed SQLite design on production-like STAC records, including large
  asset lists: total disk usage after initial filling and repeated updates,
  harvest throughput, exact/predecessor lookup latency, decompression cost, and
  complete mapping throughput.

Deferred: PostgreSQL backend for several instances, including coordinated Kafka
consumption and update ownership. Retain JSONL dumps and Handle spools initially;
evaluate optional Parquet import/export separately if measurements justify it.

## Monitoring

- [ ] When measured statistics growth warrants it, implement the deferred
  [daily statistics rollup](docs/work-stream-design.md#future-task-daily-statistics-rollup):
  maintenance aggregates older finalized batches into `daily_stats` in `piddi.db`,
  transactionally removes included summaries, and keeps `top`, recovery, retry
  guards, and project reset correct. Leave it disabled by default.
- [ ] Record deferred publication runs, including project, input batch, start and
  completion times, receipt path, counts, outcome, and concise error details.
- [ ] Derive publication backlog and output storage usage, with configurable
  warning and critical thresholds for stale heartbeats, overdue batches, and
  free disk space.
- [ ] Add a read-only `piddi status` snapshot with human-readable and JSON output.
- [ ] Add a separately supervised `piddi monitor` web service, bound to localhost
  by default, with `/health/live`, `/health/ready`, `/health`, and `/status` JSON
  endpoints.
- [ ] Serve a small, server-rendered HTML dashboard at `/` with accessible
  traffic lights, labels, concise status messages, relative and UTC timestamps,
  and automatic refresh without requiring JavaScript.
- [ ] Document health-state semantics so an idle Kafka topic remains healthy,
  degraded conditions remain distinguishable from failures, and stale status
  data is visible.
- [ ] Add monitor deployment and nginx exposure controls, with tests for HTTP
  status codes, traffic-light rendering, and redaction of sensitive configuration.

## PID and fixtures

- [ ] Reject malformed source PIDs instead of generating replacements.
- [ ] Allow fallback PIDs only where a plugin explicitly permits them; log and
  count each fallback.
- [ ] Complete PID edge-case tests, including aliases, conflicts, Handle forms,
  relationships, POST, and PATCH.
- [ ] Add sanitized fixtures for all observed PATCH events and representative
  multi-asset records; document extraction commands and source checksums.
- [ ] Add CORDEX-CMIP7 after a representative publication record is available.

## Rollout

- [ ] Benchmark ingestion and publication with representative datasets.
- [ ] Document production sizing, retention, worker counts, and Kafka
  group/offset decisions.

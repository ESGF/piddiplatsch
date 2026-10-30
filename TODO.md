# TODO

## Handle service

- [ ] Test REST publication against a disposable real Handle prefix.
- [ ] Confirm authentication, HTTPS/CA requirements, error handling, and safe
  retry behavior without exposing secrets.
- [ ] Document safe REST publication concurrency and rate limits.
- [ ] Investigate server-side batching only if the deployed service documents a
  multi-Handle endpoint with per-item results.

## Publication spool

- [ ] Define durability rules for flushing, partial writes, disk limits, and
  retention.
- [ ] Add backlog metrics, publication rate limiting, and optional watch mode.
- [ ] Add an outbox validation command that never publishes.
- [ ] Define multi-target routing and track delivery independently per target.

## Recovery and Kafka

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
- [ ] Add the DuckDB backend alongside SQLite with configurable backend and path.
  Use one database file per project so projects can be removed, rebuilt, and
  compacted independently; scope item identity by collection and item ID.
  Document backend concurrency constraints, including sequential harvest/map
  access for separate processes using a conventional local DuckDB file.
- [ ] Define and add force-refresh semantics that never blindly reapply already
  processed, potentially non-idempotent patches. Add completeness reporting
  beyond the existing per-item remote current-state recovery.
- [ ] Add project-scoped cache inspection/removal with safe database closure and
  cleanup of associated journal files. Report item counts, unresolved records,
  checkpoints, and disk usage.
- [ ] Remove retained legacy retry library helpers after compatibility review.
- [ ] Add an explicit harvest force option with diagnosed skip/checkpoint semantics
  for unresolved events; default harvest remains fail-fast.
- [ ] Add a configurable publication failure limit aligned with mapping.
- [ ] Remove remaining legacy direct-processing library paths and obsolete
  transient configuration after compatibility review.
- [ ] Run shared backend correctness tests for publications, consecutive patches,
  duplicate replay, interrupted transactions, missing bases, project isolation,
  predecessor selection, clean/force behavior, and independent project rebuilds.
- [ ] Benchmark SQLite and DuckDB on production-like STAC records, including large
  asset lists: total disk usage after initial filling and repeated updates,
  harvest throughput, exact/predecessor lookup latency, decompression cost, and
  complete mapping throughput. Choose the default from measured results.

Deferred: PostgreSQL backend for several instances, including coordinated Kafka
consumption and update ownership. Retain JSONL dumps and Handle spools initially;
evaluate optional Parquet import/export separately if measurements justify it.

## Monitoring

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

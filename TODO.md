# TODO

## Handle service

- [ ] Test REST publication against a disposable real Handle prefix.
- [ ] Confirm authentication, HTTPS/CA requirements, error handling, and safe
  retry behavior without exposing secrets.
- [ ] Compare REST and `pyhandle` output, then document safe concurrency and
  rate limits.
- [ ] Investigate server-side batching only if the deployed service documents a
  multi-Handle endpoint with per-item results.

## Publication spool

- [ ] Define a versioned outbox record with source-event identity and ordering
  metadata; document migration from the current JSONL format.
- [ ] Define durability rules for flushing, partial writes, disk limits, and
  retention.
- [ ] Add atomic file sealing/claiming, checksums, and crash recovery.
- [ ] Add `publish` watch mode with checkpoints, graceful restart, rate limiting,
  and backlog metrics.
- [ ] Add an outbox validation command that never publishes.
- [ ] Define multi-target routing and track delivery independently per target.

## Recovery and Kafka

- [x] Add per-project consumed, routed, filtered, succeeded, skipped, and failed
  counters.
- [ ] Define and test Kafka acknowledgement behavior across persistence errors,
  automatic commits, transient failures, and restarts.

## Harvest catalog cache and mapping

Design target: `harvest` reconstructs a persistent local STAC catalog from Kafka
before mapping. `map` reads the latest complete state of each versioned item from
that catalog, with no remote STAC dependency. Keep older dataset versions for
local predecessor lookup, but overwrite successive updates to the same item.

- [x] Define a shared catalog interface for exact item lookup, predecessor
  lookup, bounded-memory iteration, transactional item/checkpoint writes, reset,
  and close. Keep patch application, project routing, version parsing, and
  duplicate-message handling independent of the storage backend.
- [x] Implement the SQLite catalog with compressed JSON, indexed lookups,
  transactional checkpoints, project ownership checks, reset, and streaming reads.
- [ ] Add the DuckDB backend alongside SQLite with configurable backend and path.
  Use one database file per project so projects can be removed, rebuilt, and
  compacted independently; scope item identity by collection and item ID.
  Document backend concurrency constraints, including sequential harvest/map
  access for separate processes using a conventional local DuckDB file.
- [x] Make `harvest` recognize projects and validate publication envelopes,
  identities, patch targets/operations, and the resulting STAC item structure.
  Keep Handle-specific validation in `map`.
- [x] Attach SQLite catalogs to `harvest`, retaining raw dumps and a `--no-catalog`
  compatibility mode. Use a separate Kafka group and synchronous acknowledgement
  after durable catalog writes; stop on unresolved events and persist decoded
  failure records with source coordinates. Test duplicate patch replay and failed
  Kafka acknowledgement without reapplying catalog updates.
- [ ] Store full publication items and apply PATCH events to their cached base
  items in source order. Persist the resulting complete item for later patches.
  Establish ordering assumptions when an item can appear in several partitions
  or topics; do not assume Kafka provides global ordering.
- [ ] Record unknown projects, malformed records, and patches without base items
  as unresolved, with source coordinates and a reason; provide retry/recovery
  without silently dropping records or storing fabricated base items.
- [ ] Persist per-project Kafka topic/partition checkpoints atomically with
  catalog changes. Skip already processed messages, not existing item IDs:
  existing items must still accept new patches and replacement publications.
  Define acknowledgement and unresolved-record durability rules before advancing
  checkpoints, including topics/partitions shared by several projects.
- [ ] Add explicit clean/rebuild and force-refresh options. Reset the selected
  catalog and its checkpoints together; define force semantics that never
  blindly reapply already processed, potentially non-idempotent patches.
  Support rebuilding a removed project independently of committed Kafka group
  offsets, and report when retained Kafka history lacks required publications.
- [ ] Add project-scoped cache inspection/removal with safe database closure and
  cleanup of associated journal files. Report item counts, unresolved records,
  checkpoints, and disk usage.
- [ ] Switch `map` input from raw event replay to complete cached items and stream
  them through the shared processing pipeline. Define project selection,
  iteration order, limits/offsets, and rerun behavior for latest-state mapping.
  Document migration of existing JSONL mapping/recovery workflows.
- [ ] Replace remote patch fetching and STAC/Elasticsearch version lookup paths
  with the integrated catalog. Keep predecessor linking optional during initial
  filling, then allow remapping/reconciliation to populate missing links.
  Select the nearest strictly older version within the same project, collection,
  and dataset; distinguish disabled lookup, no predecessor, and storage failure.
- [ ] Keep full STAC JSON losslessly, with compact serialization and benchmarked
  per-item compression. Index exact identities and dataset/version metadata so
  predecessor searches do not decompress or scan item payloads. Minimize redundant
  indexes and checkpoint metadata; define compaction and journal growth controls.
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

## Processing architecture

- [x] Refactor shared processing out of `consumer.py` in a separate PR from the
  JSONL streaming fixes. Shared execution now lives in `core/pipeline.py`, with
  dedicated mapping and retry runners under `runners/`. `DirectConsumer` has
  been removed; the pipeline accepts iterables directly.

Design target:

- A shared processing pipeline accepts an iterable of keyed records and owns
  record processing, counters, failure handling, and stop policies, without
  depending on Kafka or file readers.
- The Kafka consumer owns polling and feeds records into the shared pipeline.
- A dedicated mapping runner streams raw JSONL into that pipeline; `map` no
  longer imports execution helpers from `consumer.py`.
- The retry runner streams recovery records into the same pipeline and retains
  retry-specific project routing, retry counts, and input-retention rules.
- `jsonl_stream.py` remains the shared file-reading utility, and CLI command
  classes remain thin wrappers around their runners.
- Remove `DirectConsumer` if accepting iterables directly makes it redundant.

Keep this refactor behaviour-preserving: retain bounded memory, source order,
offsets and limits, project routing, output formats, progress and counters,
error/stop policies, file cleanup, and retry deletion safeguards. Use the
existing unit, integration, and streaming regression tests as the baseline;
add focused tests for the extracted boundaries where needed.

## Monitoring

- [x] Define one shared, versioned status model for operator commands and HTTP
  responses, without exposing credentials, raw messages, or tracebacks.
- [x] Persist a consumer heartbeat independently of Kafka traffic, plus startup,
  last-message, last-success, last-error, counter, topic, group, and selected
  project information.
- [ ] Record deferred publication runs, including project, input batch, start and
  completion times, receipt path, counts, outcome, and concise error details.
- [ ] Derive publication backlog and output storage usage, with configurable
  warning and critical thresholds for stale heartbeats, overdue batches, and
  free disk space.
- [ ] Add a read-only `piddi status` snapshot with human-readable and JSON output,
  and a `piddi top` live terminal view following the Rook process-monitor pattern.
- [ ] Add a separately supervised `piddi monitor` web service, bound to localhost
  by default, with `/health/live`, `/health/ready`, `/health`, and `/status` JSON
  endpoints.
- [ ] Serve a small, server-rendered HTML dashboard at `/` with accessible
  traffic lights, labels, concise status messages, relative and UTC timestamps,
  and automatic refresh without requiring JavaScript.
- [ ] Document health-state semantics so an idle Kafka topic remains healthy,
  degraded conditions remain distinguishable from failures, and stale status
  data is visible.
- [ ] Add systemd and Ansible deployment examples, nginx exposure controls, and
  tests for heartbeat expiry, HTTP status codes, traffic-light rendering, and
  redaction of sensitive configuration.

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

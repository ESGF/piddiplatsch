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

- [ ] Add per-project consumed, routed, filtered, succeeded, skipped, and failed
  counters.
- [ ] Define and test Kafka acknowledgement behavior across persistence errors,
  automatic commits, transient failures, and restarts.

## Monitoring

- [ ] Define one shared, versioned status model for operator commands and HTTP
  responses, without exposing credentials, raw messages, or tracebacks.
- [ ] Persist a consumer heartbeat independently of Kafka traffic, plus startup,
  last-message, last-success, last-error, counter, topic, group, and selected
  project information.
- [ ] Record deferred publication runs, including project, input batch, start and
  completion times, receipt path, counts, outcome, and concise error details.
- [ ] Derive publication backlog and output storage usage, with configurable
  warning and critical thresholds for stale heartbeats, overdue batches, and
  free disk space.
- [ ] Add a read-only `piddi status` snapshot with human-readable and JSON output,
  and a `piddi top` live terminal view following the Rook `qtop` pattern.
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

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
- [ ] Add file rotation, atomic sealing/claiming, checksums, and crash recovery.
- [ ] Add `publish` watch mode with checkpoints, graceful restart, rate limiting,
  per-PID ordering, and backlog metrics.
- [ ] Add an outbox validation command that never publishes.
- [ ] Define multi-target routing and track delivery independently per target.

## Recovery and Kafka

- [ ] Store project/plugin identity in failure and skipped records; make their
  paths and retries project-scoped.
- [ ] Add per-project consumed, routed, filtered, succeeded, skipped, and failed
  counters.
- [ ] Define and test Kafka acknowledgement behavior across persistence errors,
  automatic commits, transient failures, and restarts.

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

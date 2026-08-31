# TODO

The shared-topic router, built-in CMIP6/CORDEX-CMIP6/CMIP7/CMIP6Plus
plugins, staged `harvest`/`map`/`publish` commands, project-scoped Handle JSONL,
and concurrent single-Handle REST publisher are implemented. The remaining work
is below, in priority order.

## 1. Validate the real Handle service

- Obtain a disposable prefix, service URL/version, and credentials through an
  approved secret channel. Keep secrets out of logs and the repository.
- Run the opt-in live suite for create, resolve, overwrite, parallel publishing,
  and repeated updates to one PID.
- Confirm whether HTTP Basic authentication works. If the service requires
  Handle challenge/session authentication, use the complete `pyhandle` backend
  instead of copying private cookies, nonces, or headers.
- Require HTTPS for authenticated remote writes. Add custom CA or client
  certificate settings only if the deployed service needs them.
- Compare equivalent records written through REST and `pyhandle`, including
  values, indices, administrator permissions, encoding, and TTLs.
- Classify authentication, authorization, validation, rate-limit, timeout,
  connection, and server failures; document which are safe to retry.
- Benchmark REST worker counts and `pyhandle`. Document a safe production
  concurrency/rate limit and whether the observed limit is global, per
  credential, or per connection.
- Define retention or cleanup for the uniquely named Handles created by live
  tests.

Only add server-side multi-Handle batching if the deployed service exposes a
documented, tested endpoint with item-level outcomes. Otherwise keep
`publish_many()` as bounded orchestration over single-Handle PUTs.

## 2. Build a durable publication spool

### Outbox format and durability

- Replace the diagnostic Handle JSONL format with a versioned, target-neutral
  envelope containing the operation, full Handle, values, project/plugin,
  source-event identity, ordering metadata, creation time, and schema version.
- Define migration rules so old audit JSONL cannot be published accidentally.
- Specify flush/fsync behavior, partial-line recovery, free-disk limits, and
  backpressure. Keep raw dumps, outbox records, mapping failures, and delivery
  failures as separate artifact types with separate retention rules.
- Add an inspection command that validates outbox schema and integrity without
  publishing.

### File lifecycle

- Rotate and atomically seal active files before publication. Use a recoverable
  lifecycle such as `writing -> ready -> processing -> published/failed`.
- Choose size/count/time rotation thresholds and write a manifest with record
  count and checksum for every sealed file.
- Claim files atomically so multiple publishers cannot process the same file;
  recover claims left by crashed workers.

### Publisher watch mode

- Add a long-running `piddi publish` spool/watch mode for sealed files.
- Persist line-level delivery progress while keeping the outbox immutable.
- Preserve update order for each PID across files, restarts, and publisher
  processes. Decide whether dataset Handles must precede their file Handles.
- Treat publication as idempotent, at-least-once upsert; handle ambiguous
  connection loss without assuming the server did not commit.
- Support rate limiting, graceful draining, restartable checkpoints, and
  metrics for backlog age/size, throughput, retries, failures, and target lag.
- Test crash points around append, seal, claim, checkpoint, request completion,
  archive, restart, corrupt input, poison records, disk-full behavior, and
  concurrent workers.

### Multiple Handle targets

- Define explicit per-project routing: one target, fan-out, or different targets
  by project/prefix. Do not infer fan-out from configured profiles.
- Track delivery independently per target, preferably with one immutable outbox
  and a per-target ledger rather than copied JSONL files.
- Test recovery with two targets when one is slow or unavailable.

## 3. Finish recovery and Kafka correctness

- Store canonical project/plugin identity in skipped and failure records. Make
  retries select that plugin deterministically and use project-scoped paths.
- Report consumed, routed, filtered, succeeded, skipped, and failed totals, with
  per-project routed breakdowns.
- Define the Kafka acknowledgement boundary. A filtered event is complete; a
  routed event is complete only after its outbox write or direct publication
  satisfies the persistence policy.
- Document and test the interaction with `enable.auto.commit`, transient
  fail-fast behavior, restarts, and at-least-once Handle updates.

## 4. Tighten PID validation and fixtures

- Distinguish missing PIDs from malformed or unrecognized values. Never replace
  a present invalid PID with a generated one.
- Generate a deterministic fallback only when a plugin explicitly permits a
  missing PID; log and count every fallback.
- Add focused tests for equal/conflicting canonical and legacy fields, missing
  versus malformed values, Handle URI/prefix forms, relationship consistency,
  and POST/PATCH processing after STAC retrieval.
- Expand the sanitized queue fixtures to include all four observed PATCH events
  and representative multi-asset datasets/Handle output. Record the extraction
  command and source dump checksum so fixtures can be refreshed reproducibly.
- Add CORDEX-CMIP7 only after a representative publication record is available.

## 5. Production sizing and rollout

- Benchmark ingestion and publication separately with representative asset
  counts.
- Document disk sizing, retention, rotation, publisher worker count, and Kafka
  consumer-group/offset decisions for deployment.

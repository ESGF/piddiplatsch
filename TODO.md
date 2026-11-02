# TODO

Open work only. The implemented single-database design and its recovery rules are
recorded in [work-stream design](docs/work-stream-design.md); current commands,
monitoring, retention, and disk safety recovery are in [operations](docs/operations.md).
The four-table schema requires a fresh start; no legacy migration is planned.

## Production validation and sizing

- [ ] Diagnose the recorded CMIP6Plus mapping errors from the production test;
  add sanitized regression fixtures for any unsupported valid input.
- [ ] Measure the single SQLite database on representative STAC records, including
  large asset lists: catalog/work/index space, growth per accepted event,
  repeated updates, harvest and mapping throughput, lookup latency,
  decompression cost, writer contention, and backup/restore time. Reassess
  feasibility from measurements rather than a fixed database-size cutoff.
- [ ] Measure publication throughput and establish safe REST worker counts and
  rate limits for the deployed Handle service. Run the existing opt-in real-service
  tests against a disposable prefix; verify authentication, HTTPS/CA handling,
  failures, and retry behavior without exposing secrets.
- [ ] Validate Kafka persistence and restart behavior with a real broker,
  including commit failures, rebalances, interrupted writes, and VM restarts.
  Local checkpoint, replay, and recovery tests already exist; automatic offset
  commits and offset storage are disabled.
- [ ] Validate the persistent disk safety stop on the deployed services: trigger
  configured thresholds on a disposable filesystem, verify restart/timer/reboot
  blocking, then inspect and manually resume. Do not fill the production disk.
- [ ] Document measured production sizing, backup/restore procedures, worker
  counts, and Kafka group/offset decisions. Evaluate published-work and completed
  output retention using measured growth; both removal policies remain disabled
  by default. Verify that deleted work space is reused without automatic VACUUM.

## Publication

- [ ] Add configurable publication rate limiting and a failure limit aligned
  with mapping.
- [ ] Add an outbox validation command that never publishes.
- [ ] Investigate server-side batching only if the deployed service documents a
  multi-Handle endpoint with per-item results.
- [ ] Define multi-target routing and track delivery independently per target.

## Harvest, catalog, and recovery

Harvest maintains the current catalog; mapping consumes the immutable snapshots
saved with each work event. Dumps preserve original events and source coordinates.
Unknown projects already support stop/ignore behavior; other invalid harvest
inputs remain fail-fast.

- [ ] Document ordering assumptions for updates to the same item across Kafka
  partitions or topics, including the limits of per-partition checkpoints.
- [ ] Define an explicit operator recovery policy for unresolved or invalid
  harvest events, including diagnosed skip/checkpoint semantics. If an optional
  acknowledge-and-continue mode is added, preserve the original archive and
  record the reason; default harvest remains fail-fast.
- [ ] Define force-refresh semantics that never blindly reapply already
  processed, potentially non-idempotent patches. Add completeness reporting
  beyond the existing per-item remote current-state recovery.
- [ ] Add read-only project inspection for catalog counts, unresolved records,
  checkpoints, and storage use. Define any catalog-removal operation separately
  from the existing processing reset, with safe shared-checkpoint semantics.

## Monitoring

`top` already reads the shared database for event totals, full/patch counts,
prepared/published Handles (including aliases), backlog, rates, batch progress,
and overall publish ETA. Batch summaries and run histories are already recorded.
Disk warnings and the persistent critical-space stop are implemented.

- [ ] Add output storage usage reporting and configurable health thresholds for
  stale heartbeats and overdue batches. Make stale snapshots visible while
  distinguishing healthy idle harvest from failures.
- [ ] Add a separately supervised `piddi monitor` web service, bound to localhost
  by default, with `/health/live`, `/health/ready`, `/health`, and `/status` JSON
  endpoints using the existing database snapshot model.
- [ ] Serve a small, server-rendered HTML dashboard with accessible status labels,
  concise messages, relative and UTC timestamps, and automatic refresh without
  requiring JavaScript.
- [ ] Document monitor deployment and nginx exposure controls; verify HTTP status
  codes, health-state rendering, and redaction of sensitive configuration.

## PID and fixtures

- [ ] Reject malformed source PIDs instead of generating replacements. Allow
  fallback PIDs only where a plugin explicitly permits them; log and count each
  fallback.
- [ ] Extend existing PID tests for uncovered aliases, conflicts, Handle forms,
  relationships, POST, and PATCH cases found in production.
- [ ] Add sanitized fixtures for observed PATCH variants and representative
  multi-asset records; document extraction commands and source checksums.
- [ ] Add CORDEX-CMIP7 after a representative publication record is available.

## Deferred until measurements justify them

- [ ] Parallel project processing: retain sequential round robin batches for now.
  Before adding a bounded project-worker pool, define durable claims, recovery,
  per-project ordering, and SQLite writer-contention limits. Projects do not
  write to the same Handle.
- [ ] Implement the deferred
  [daily statistics rollup](docs/work-stream-design.md#future-task-daily-statistics-rollup)
  only when batch-summary growth warrants it. Maintenance would aggregate older
  finalized batches into `daily_stats` in the same database, preserving totals,
  recovery, retry guards, and project reset. Leave it disabled by default.
- [ ] Consider PostgreSQL only if measured scale or multiple service instances
  require it, including coordinated Kafka consumption and update ownership.
  Separate per-project databases are not the current design.
- [ ] Evaluate optional Parquet import/export only if measurements justify it;
  retain JSONL dumps and Handle spools for now.

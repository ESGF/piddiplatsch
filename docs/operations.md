# Operations

## Fresh start

Stop harvest and batch services. Archive the previous output directory and old
monitoring database, then configure a new empty output directory when migrating
from per-project catalogs, dirty-item queues, or incompatible manifests. Schema
4 upgrades to schema 5 automatically, without a fresh start: the new SQLite
outbox tables are added, and ready manifest-v3 batches and their per-Handle
journals are imported transactionally when processed. Keep those legacy files
until imported. Stop all services when upgrading the application; older code
does not support schema 5. Preserve the database and harvest control file.

## Disk safety recovery

By default, Piddi warns below **40 GiB** free and stops new work below **20 GiB**.
Checks during processing are throttled to ten seconds. A critical reading or an
unavailable storage check writes `.disk-space-stop` in the configured output
directory. Harvest, mapping, publication, and maintenance honor this persistent
stop. Deployed systemd services skip automatic restarts and timer starts while it
exists, including after a reboot. Already running jobs stop at their next disk
check; writes already in flight may finish.

For the standard deployment, inspect the marker and journal, stop the services,
and check the mounted filesystem. After resolving the cause and restoring enough
headroom (preferably above the warning threshold), explicitly resume:

```bash
cat /var/lib/piddi/.disk-space-stop
systemctl stop piddi.service piddi-sync.service piddi-maintenance.service
df -h /var/lib/piddi
# After inspection and freeing space:
rm /var/lib/piddi/.disk-space-stop
systemctl start piddi.service piddi-sync.service
```

The marker is outside the database; no database reset is needed. Deploy updated
service units, run `systemctl daemon-reload`, and restart running services to
enable this behavior. Timers remain enabled and resume their normal schedules
after the marker is removed.

## Harvest

```bash
piddi harvest
piddi harvest --idle-timeout 0
```

Harvest archives each Kafka event before committing its reconstructed catalog
item, full work snapshot, and shared topic/partition checkpoint in `piddi.db`.
One harvest process owns the configured source. Map and publication can run while
harvest continues. New events do not change already queued snapshots.

An invalid event or persistence failure stops harvest without acknowledging it.
The dump archive preserves topic, partition, offset, key, and original payload.
Files rotate by UTC day and `consumer.dump_max_events` (default 100,000).

Unknown projects also stop harvest by default. Use
`piddi harvest --ignore-unknown-projects` (or set
`catalog.ignore_unknown_projects = true`) to archive them in
`dump/unknown_projects_<day>_<part>.jsonl`, count them as filtered by project,
and continue. Use `--stop-on-unknown-projects` to force stopping for one run.
Every ignored event logs a warning with its source coordinates.
`top --json` exposes the run's per-project filtered counts. No catalog or work
entry is created for an unknown project. The archive write must succeed before
the shared checkpoint and Kafka acknowledgement advance.

The normal all-event dump is still written. Dedicated unknown archives make
inspection and future replay easier. A crash before checkpoint commit can leave
duplicate archive entries; use topic/partition/offset to identify unique events.
If support is added later, replay into a fresh database: ordinary replay into
the current database skips already checkpointed events. Missing/conflicting
project identities and malformed envelopes remain errors in either policy.

## Map and inspect

```bash
piddi map --project cmip6
piddi map --all-projects --limit 2000
```

Map selects pending database work, in ascending ID order within each project.
It processes one bounded project batch per round, round robin. It never contacts
Kafka, STAC, or the Handle server. Date selectors refer to local work creation
days in UTC; omitted dates make all pending work eligible. The total per-command
limit defaults to `[batch].max_items` (2,000), across all selected projects.
Use `--limit N` to override it, including a larger number that spans several
batches. Offset/limit count work entries after date filtering. Successfully sealed work is not
mapped again by a repeated command.

Inspect `batches/<project>/ready/<batch-id>/handles.jsonl`, `failures.jsonl`, and
`manifest.json`. The manifest records settings, checksums, exact work IDs, and
mapping outcomes. All Handles for one work entry stay together. These files are
audit exports; the work transitions, queued payloads, and batch counters commit
together in SQLite. Once committed, publication does not depend on the exports.

## Publish and sync

```bash
piddi publish --project cmip6 --workers 4 --retries 3
piddi sync
piddi sync --force-map
piddi sync --project cmip6
```

Publish without paths sends saved queued batches and updates their database
states. It does not map. Sync maps eligible pending work and publishes saved
batches. The threshold is 100 pending work entries or 10 minutes; force-map skips
the wait. The default ceiling is 2,000 work entries per project batch.

Sync runs projects sequentially, choosing each next batch by the eligible
projects' `plugins.<project>.priority` weights (default 100, range 0–255).
Zero disables sync for that project, including forced sync; manual map/publish
remain available. Higher weights increase selection probability. Requests for
different Handles within the current batch may run concurrently. Projects never
write the same Handle.
Failed publication blocks later publication for its project while other projects
continue. Manual mapping of newer work cannot bypass older pending work: map that
older work or use sync before publishing the newer batch.

Publication streams immutable Handle payloads from `handle_outbox`; successful
positions commit to SQLite before a later update to the same Handle starts.
Retries skip committed positions and do not remap. A failed write defers later
updates to that Handle for the attempt. Receipts go under
`published/YYYY/MM/DD/`. Successful batches move to
`batches/<project>/published/YYYY/MM/DD/<batch-id>/`. A crash may repeat HTTP
writes against the same Handle ID. SQLite work-state changes, completion counters,
and removal of finished outbox payloads commit together. No transaction is held
while waiting for HTTP. Exports and receipts are not needed to resume a committed
queue. Standalone file publication continues to support its optional file journal.

Explicit `piddi publish PATH...` is available for operator-managed JSONL files.
It does not acknowledge database work or add to the work-stream batch totals.
Use queued publication for mapped work. Queued publication processes complete
batches; date, limit, and offset selectors cannot bypass its ordering.

## Invalid work and replay

Invalid mapping snapshots stay in the database with an explanation. They do not
automatically return to pending. A correction arriving on Kafka is a new snapshot.
For a software fix before successful publication:

```bash
piddi retry --project cmip6 12 13
piddi map --project cmip6
```

Finish the original saved batch first. Retry refuses work when newer snapshots
were mapped/published or the project has any recorded successful publication.
This conservative policy also protects against stale retries after retention.

For a full ordered replay of a project's retained work, stop harvest and all
batch commands, preview and then apply:

```bash
piddi admin reset --project cmip6
piddi admin reset --project cmip6 --apply
# Or reset only a recent suffix (last seven UTC calendar days, including today):
piddi admin reset --project cmip6 --from today-6
piddi admin reset --project cmip6 --from today-6 --apply
```

Reset updates work rows in place without copying the shared database. It moves
that project's old output into `reset-archives/<timestamp-id>/`, clears that
project's batch summaries, and requeues all retained event snapshots. Catalog
records and Kafka checkpoints stay unchanged. Work already removed by
retention cannot be recovered by this reset.

`--from YYYY-MM-DD` (also `today`, `yesterday`, `today-N`) selects the UTC day of
local work creation, inclusively, and all later work IDs. It uses `created_at`,
so publication, retries and their `updated_at` timestamps do not change selection.
There is no end date: replay always continues through the retained suffix.
The start expands to include whole affected batches; preview shows the effective
timestamp, first work ID and count. Earlier completed work, batches and batch
summaries remain unchanged. Run summaries overlapping the effective boundary are
cleared as a whole because their counters cannot be split reliably. Shared receipt
logs are copied to the reset archive and kept for unaffected batches/projects.
If no rows match, apply does nothing. If part of an affected batch was removed
by retention, dated reset refuses: restore missing history from dump backups
before using that selection. Omitting `--from` retains the full reset behavior.

Manual state-rewriting jobs are grouped under `piddi admin`:

```bash
piddi admin --help
piddi admin reset --project cmip6
piddi admin rebuild
```

Every admin job previews by default. Add `--apply` to execute `reset`, `rebuild`
or `recover`. This confirms execution; it does not bypass their safety checks.

For occasional manual consolidation of the live work table, use
`piddi admin rebuild` to preview, then `piddi admin rebuild --apply` after stopping
services and finishing all pending work/publication. It archives the old history
and batches, and retains one published snapshot per catalog item without
reprocessing. Invalid work must also be resolved first. Add `--vacuum` only when file space should also be
returned to the filesystem. See [manual work-history rebuild](recovery.md#manual-work-history-rebuild)
for storage requirements and interruption recovery.

Original archives are an alternative harvest input:

```bash
piddi harvest --archive outputs/dump/dump_messages_2026-11-01_000001.jsonl
piddi admin recover --archive /backup/piddi/dump --apply
```

Repeat `--archive` for multiple parts, including `.jsonl.gz` files. Parts sort by
name; checkpoint deduplication prevents patches from being applied twice. Replay
requires the new coordinate-bearing envelope; legacy payload-only dumps are not
supported. Rebuilding shared harvest state must include all stored projects;
`piddi admin recover --clean-catalog --apply` starts a new Kafka group generation and
resets the shared state. `--clean-project` also belongs to `piddi admin recover`;
normal `piddi harvest` does not expose destructive cleanup options.
A partial harvest clean is refused when other stored projects share checkpoints.
Normal Kafka harvest resumes each partition at its database checkpoint,
independently of broker group offsets, and stops if required history has expired.
See [dump backup recovery](recovery.md#rebuild-from-dump-backups-then-catch-up-with-kafka)
for the manual rebuild and Kafka handoff procedure.

## Remote current-item recovery

If a PATCH has no local base, harvest can fetch the current remote STAC item
using the configured shared/project STAC endpoint. It does not apply the queued
patch to the fetched state. Recovered items retain local provenance and stay in
remote-refresh mode for later events and startup refreshes. Fetch failures leave
catalog, work, and checkpoint changes uncommitted. Mapping stays offline.

## Monitoring and maintenance

```bash
piddi top
piddi top --once --json
piddi maintain --dry-run
piddi maintain
```

Top reads only `piddi.db`, including when `--db PATH` selects another database.
The SQLite table sizes are rough estimates, include indexes, and cover all
projects, even with `--project`. Top estimates the application tables, including
the outbox, from
up to 16 rows per project/table and assumes 75% page occupancy. Estimates can
be biased by changing row sizes and fragmentation; they are not measured disk
allocations. JSON labels them with `estimated: true` and includes the method
and sample counts. Catalog and queue row counts remain exact, using covering
indexes without loading STAC blobs or counting pending event types.
Interactive top samples counts and estimated sizes in a background
thread, then waits a minute after completion before sampling again. Live
activity keeps refreshing while scans run. Until the first result, counts show
as unknown; subsequent scans retain the last values and show their sample time.
`--once` and `--json` use the same fast estimates. JSON includes `table_sizes` with
per-table `table_bytes`, `index_bytes`, and `total_bytes`. Sizes exclude the WAL,
database schema, and unallocated free pages. No `dbstat` scan is performed.
Catalog items count distinct stored STAC items. The summary shows
`Mapped: N / total events`, where total is successfully mapped + currently pending
+ currently invalid events. This is all accepted queued event history, rather
than the number of distinct catalog items or received Kafka deliveries. Several
events can refer to the same catalog item. Filtered/replayed deliveries, ignored
unknown projects, and publication-attempt errors do not add to the denominator.
Explicit invalid retries move the same event between states without increasing
the total. Mapped counts survive published-work cleanup in retained batch stats;
project reset clears that project's mapped history and requeues retained work.
`top --json` exposes `event_totals` with `mapped`, `pending`, `invalid`, and `total`,
respecting `--project` when selected.
The `Batch:` line shows the configured event-count trigger, oldest-event wait
trigger, and maximum events per batch (defaults: 100 events or 10m; max 2,000).
Between batches, Activity shows `idle` when there is no work, `waiting` when
pending events are below both mapping triggers, and `ready` once the event count
or oldest event age reaches the configured `[batch].trigger_items` or
`max_wait_seconds` (defaults: 100 events or 10 minutes). `ready` means eligible
for the next scheduler selection, with project priority determining its weight;
it is not a promise that the project will run next. `queued` means mapping has
already produced a batch awaiting publication. Priority-zero projects with work
show `disabled`. Counts initially show `sampling`; `pending` is a fallback when
an incomplete snapshot lacks the oldest event timestamp. Counts are sampled in
the background; age eligibility is reevaluated on each refresh. The same config
used by sync must be loaded by top for these thresholds and priorities to match.

The Activity column shows `preparing`, `mapping`, `sealing`, `verifying`,
`publishing`, or `finalizing` throughout a batch. A separate heartbeat stays
active during file checks, durable writes, and database acknowledgements;
`pending` no longer stands in for these stages. Very short phases can still
finish between screen refreshes. JSON includes the lifecycle as a `batch` run
with `phase` and `selected_projects`, separate from throughput counters.
If the activity heartbeat becomes stale, the last phase remains visible with a
stale warning. Failed batch stages close their activity record and raise an error
alert, including failures after mapping has finished.

Below the table, one `Avg ~ 10m/30m/1h` line shows overall harvest speed in
received events/s, mapping speed in successfully mapped events/s, and publication
speed in Handles/s for the last 10 minutes, 30 minutes, and one hour. Rates combine
existing samples across batches and projects, weighted by time spent in each operation. `--project`
restricts the meter to that project. Time between runs is excluded;
stalls within a run count. These are processing speeds, rather than total
throughput including idle time. Short histories use the available samples;
`—` means no measured interval in that window, while `0.0` means observed zero
progress. Window boundaries are approximate between samples, and stale runs
contribute only through their last heartbeat. No extra database writes are needed.
JSON exposes combined `processing_rates`, with `harvest`, `map`, and `publish`
entries for `10m`, `30m`, and `1h`, each containing `per_second` and the measured
`active_seconds`.
The live `Handles/s`, batch ETA, and five-minute harvest average are unchanged.

The harvest summary adds `stored: N full / M patch` for the displayed harvest
runs. These counts include only successfully stored events. The received-event
total also includes filtered/replayed deliveries and failures, so it can be larger.
`top --json` includes `full_events` and `patch_events` in run counters, history
deltas, and processed-batch totals, plus `pending_full_events` and
`pending_patch_events` in each project's health. Batch type totals include both
successfully mapped and rejected events. Mapping uses full snapshots while
retaining the original source event type for statistics.
Prepared and published Handle totals include dataset, file, and alias Handles.
One retained summary per batch supplies mapped-work, prepared-Handle,
published-Handle, and error totals, even after work retention. Pending entry counts
come from work rows; ready batch counts come from batch summaries. Publication
failures remain ready; mapping failures appear as invalid work or failed runs.
Live heartbeats and rate samples occupy separate `run` rows in `stats`. Disk space
is the last heartbeat snapshot, rather than a fresh filesystem check by top.
`maintain` prunes completed run histories according to `stats.sample_retention_days`
without deleting batch summaries. Last publication comes from the latest
published batch row.

The overall publication ETA appears below the table and covers only the current
backlog, using recent Handle-write speed and observed Handles per mapped event.
Recent mapping runs supply that ratio; retained batch totals provide a fallback
when run history has been cleaned up. During the first 30 seconds of a healthy
new batch, the overall ETA can reuse the latest successful batch's speed for the
same project(s), provided it finished within five minutes and has sufficient
samples. The line labels this as recent batch speed during warm-up; current speed
replaces it as soon as available. Stale publishers, publication errors, and longer
stalls do not use that fallback. JSON identifies the speed source in `rate_source`.
Future harvest arrivals and mapping time are not forecast. If some pending work lacks a ratio or counts are unavailable,
`top` shows a **partial** estimate for the known portion and names the unestimated
projects, their pending event counts when known, and the reason. Known ready
Handles remain included even when that project's pending events cannot be
estimated. Idle projects do not block the estimate. When publication is inactive,
speed samples are unavailable, or errors prevent an estimate, the overall ETA
line displays the reason instead of disappearing. JSON exposes `partial`,
`estimated_projects`, and `unestimated_projects` alongside the estimate.

Maintain compresses closed dump files and completed output. By default,
`maintenance.compress_outputs = true` gzips archived batch JSONL and closed
publication receipts, while `maintenance.output_retention_days = 0` keeps them
indefinitely. Set the latter to `30` to remove older completed output. Completion
time controls batch age, so a batch created weeks ago but published today is kept.
Standalone receipts use their last-write time. Ready/failed batches, their
referenced receipts, and actively written receipts are protected.

Archived manifests stay readable and link to compressed receipts. Their payload
checksums still describe the original JSONL bytes; use `gzip -cd FILE.jsonl.gz`
for inspection or decompress a copy for manual tooling that needs plain JSONL.
This does not change normal publication/recovery, which uses uncompressed ready
batches. Maintenance coordinates with project reset, and can compress completed
archives while harvest and sync continue. An interrupted archive deletion is
resumed on the next run with output retention enabled.

Run `piddi maintain --dry-run` to see planned paths before enabling removal.
These settings do not remove Kafka dumps or DB summaries. Database work cleanup
is implemented but disabled by default (`batch.retention_days = 0`), so all work rows remain.
To enable it later, set a positive period, such as 30 days. Cleanup then removes
only expired published work, using the time it entered published state
(`updated_at`). It keeps pending, mapped, invalid, catalog, summary, and output data.
It never runs `VACUUM`. Physical shrinking requires a separate supervised offline
operation with a database backup and sufficient free disk space.

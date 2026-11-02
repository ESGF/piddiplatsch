# Operations

## Fresh start

Stop harvest and batch services. Archive the previous output directory and old
monitoring database, then configure a new empty output directory. There is no
migration from per-project catalogs, dirty-item queues, older shared database
schemas, or older batch manifests. Full/patch event tracking requires schema 4;
restart tests with a new empty output directory, rather than a project reset.
Keep each new database paired with its batch directory and harvest control file.

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
days in UTC; omitted dates select all pending work. Offset/limit count work entries
across selected projects, after date filtering. Successfully sealed work is not
mapped again by a repeated command.

Inspect `batches/<project>/ready/<batch-id>/handles.jsonl`, `failures.jsonl`, and
`manifest.json`. The manifest records settings, checksums, exact work IDs, and
mapping outcomes. All Handles for one work entry stay together.

## Publish and sync

```bash
piddi publish --project cmip6 --workers 4 --retries 3
piddi sync
piddi sync --force-map
piddi sync --project cmip6
```

Publish without paths sends saved queued batches and updates their database
states. It does not map. Sync maps eligible pending work and publishes saved
batches. The threshold is 1,000 pending work entries or one hour; force-map skips
the wait. The default ceiling is 2,000 work entries per project batch.

Projects run sequentially, round robin. Requests for different Handles within
the current batch may run concurrently. Projects never write the same Handle.
Failed publication blocks later publication for its project while other projects
continue. Manual mapping of newer work cannot bypass older pending work: map that
older work or use sync before publishing the newer batch.

Publication uses saved output and a durable per-Handle success journal; retries
skip completed positions and do not remap. A failed write defers later updates to
that Handle for the attempt. Receipts go under
`published/YYYY/MM/DD/`. Successful batches move to
`batches/<project>/published/YYYY/MM/DD/<batch-id>/`. A crash may repeat HTTP
writes, but database acknowledgements and batch summaries are idempotent.

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
piddi reset --project cmip6
piddi reset --project cmip6 --apply
```

Reset updates work rows in place without copying the shared database. It moves
that project's old output into `reset-archives/<timestamp-id>/`, clears that
project's batch summaries, and requeues all retained event snapshots. Catalog
records and Kafka checkpoints stay unchanged. Work already removed by
retention cannot be recovered by this reset.

Original archives are an alternative harvest input:

```bash
piddi harvest --archive outputs/dump/dump_messages_2026-11-01_000001.jsonl
```

Repeat `--archive` for multiple parts, including `.jsonl.gz` files. Parts sort by
name; checkpoint deduplication prevents patches from being applied twice. Replay
requires the new coordinate-bearing envelope; legacy payload-only dumps are not
supported. Rebuilding shared harvest state must include all stored projects;
`--clean-catalog` starts a new Kafka group generation and resets the shared state.
A partial harvest clean is refused when other stored projects share checkpoints.

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
Catalog items count distinct stored STAC items. Pending and mapped events count
processing events; several events can refer to the same catalog item.
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

Maintain compresses closed dump files. Database work cleanup is prepared but
disabled by default (`batch.retention_days = 0`), so all work rows remain.
To enable it later, set a positive period, such as 30 days. Cleanup then removes
only expired published work, using the time it entered published state
(`updated_at`). It keeps pending, mapped, invalid, catalog, summary, and output data.
It never runs `VACUUM`. Physical shrinking requires a separate supervised offline
operation with a database backup and sufficient free disk space.

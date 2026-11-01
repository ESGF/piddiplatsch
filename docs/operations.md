# Operations

## Fresh start

Stop harvest and batch services. Archive the previous output directory and old
monitoring database, then configure a new empty output directory. There is no
migration from per-project catalogs, dirty-item queues, or older batch manifests.
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
writes, but database acknowledgements and daily summaries are idempotent.

Explicit `piddi publish PATH...` is available for operator-managed JSONL files.
It does not acknowledge database work or add to the work-stream daily totals.
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

Reset backs up the shared database and archives that project's old output. It
preserves catalog records and Kafka checkpoints, clears that project's daily
summaries, and requeues all retained event snapshots. Work already removed by
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

Top reads the shared database and live status files beside it. Daily mapped-work,
published-Handle, and error totals are independent of retained work. Live heartbeat
and rate samples are expendable monitoring data.

Maintain compresses closed dump files and removes only expired published work,
using the time it entered published state (`updated_at`). The default retention
is 30 days. It keeps pending, mapped, invalid, catalog, summary, and output data.
It never runs `VACUUM`. Physical shrinking requires a separate supervised offline
operation with a database backup and sufficient free disk space.

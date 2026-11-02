# Proposed catalog and work-stream design

Agreed and implemented on 2026-11-01. This describes the fresh-start storage
format. Existing per-project catalogs, monitoring databases, and batch manifests
are not migrated. Start with an empty output directory.

## Storage and responsibilities

Use one SQLite database, `piddi.db`, with WAL mode. Keep catalog records, work
entries, Kafka checkpoints, and summary statistics in separate tables in that
database. Project columns support filtering and project-scoped operations.

Harvest runs independently of mapping and publication. For each event:

1. Archive the original Kafka event in JSONL before further processing.
2. Reconstruct the full STAC item by storing a POST or applying a PATCH.
3. In one database transaction, update the catalog, insert an independent full
   STAC snapshot into the work stream, and advance the Kafka checkpoint.

The catalog update, work entry, and checkpoint must commit together or roll back
together. Later patches update the catalog and create new work entries; they
never change snapshots in earlier work entries. Work entries represent events,
including successive updates to the same item, rather than one coalesced pending
entry for that item.

## Tables

### `catalog_items`

Current STAC records for applying patches and local version/predecessor lookups.

| Column | Purpose |
| --- | --- |
| `project` | Project |
| `collection_id` | STAC collection |
| `item_id` | STAC item ID |
| `dataset_id` | Dataset identity without its version |
| `version_number` | Numeric version for quick version lookups |
| `stac_blob` | Full STAC record, compressed with zlib |
| `last_update` | When harvest last updated this catalog entry locally |

Primary key: `(project, collection_id, item_id)`.

Version lookup index: `(project, collection_id, dataset_id, version_number)`.
Source timestamps remain inside the STAC record.

### `work_items`

Full event snapshots and their processing progress.

| Column | Purpose |
| --- | --- |
| `id` | Unique local job number and processing order |
| `project` | Project |
| `topic` | Kafka topic |
| `partition` | Kafka partition |
| `offset` | Kafka event offset |
| `stac_blob` | Independent full STAC snapshot, compressed with zlib |
| `event_type` | Original source event: `full` (POST, including omitted method) or `patch` (PATCH) |
| `state` | `pending`, `mapped`, `invalid`, or `published` |
| `created_at` | When the job was stored |
| `updated_at` | When its state last changed |
| `batch_id` | Saved Handle batch; empty before mapping |
| `message` | Processing note or error explanation; empty when unnecessary |
| `handle_count` | Prepared Handles for this work entry, including any alias; zero before mapping |

Primary key: `id`.

Unique event: `(topic, partition, offset)` within the configured Kafka source.

Selection index: `(project, state, id)`.

The snapshot and event identity stay unchanged during retries. Item IDs and
source timestamps are available inside the snapshot; separate item ID columns,
attempt counters, detailed output references, and extra timestamps are not part
of this initial work schema. Local job order does not establish a global Kafka
ordering across partitions.

### `kafka_checkpoints`

| Column | Purpose |
| --- | --- |
| `topic` | Kafka topic |
| `partition` | Kafka partition |
| `next_offset` | Next event to read |

Primary key: `(topic, partition)`. A partition can contain several projects, so
checkpoints are not project-specific.

### `stats`

One durable summary per batch, retained independently of completed work entries.
Live monitoring uses separate rows marked `run`, within the same table.

| Column | Purpose |
| --- | --- |
| `kind` | `batch` for durable processing counts; `run` for live monitoring |
| `project` | Project; empty only for a monitoring run with no projects yet |
| `id` | Batch ID or monitoring run ID, according to `kind` |
| `status` | Batch: `ready`, `invalid`, `publication_failed`, or `published`; run: current run status |
| `created_at`, `updated_at` | First recording and latest update, in UTC |
| `mapped_at`, `published_at` | Mapping acknowledgement and full publication completion |
| `items`, `invalid_items` | Successfully mapped and rejected work entries |
| `full_events`, `patch_events` | Original types of all processed events in this batch, including rejected ones |
| `prepared_handles` | All prepared Handle records, including aliases |
| `alias_handles` | Alias subset of prepared Handles |
| `handles` | Published Handle records; assigned on full batch completion |
| `errors` | Rejected items plus failed publication attempts |
| `message` | Latest publication failure; cleared on successful completion |
| `runtime` | Run rows only: heartbeat, project counters, bounded rate samples, disk snapshot |

Primary key: `(kind, project, id)`. Mapping inserts the batch row in the same
transaction as its work-state changes. Publication updates that row. Repeating a
successful acknowledgement never creates another row or adds the counts again;
each failed publication attempt increments `errors`. Partial publication remains
visible in live run counters; durable published totals advance on batch completion.
Batch rows contain no runtime samples. Run rows have zero processing totals and
never contribute to batch aggregates. Daily mapping and publication reports can
group batches by `mapped_at` and `published_at`, respectively.

Harvest stores `event_type` with each immutable work snapshot, outside the source
STAC JSON. A PATCH remains a patch event even when harvest recovers its current
state remotely; mapping always receives the reconstructed full snapshot. Batch
type counts are derived from stored work, not from the POST envelope used for
mapping, and sum to `items + invalid_items`. Repeated acknowledgement and
publication do not increment them. Reset/retry preserve the original event type.

## Mapping and publication

A batch is a limited selection of work entries, filtered by project and state.
Mapping validates the saved snapshots and writes Handle output to JSONL files.
Keep those files; there is no `work_handles` table in this design.

- `pending`: awaiting mapping.
- `mapped`: Handle JSONL output has been durably saved and associated through
  `batch_id`; awaiting publication.
- `invalid`: mapping/validation failed; `message` explains the failure. An
  explicit retry can move an entry back to `pending`, for example after a Piddi
  software fix. Do not assume every failure requires changing source metadata.
- `published`: all Handle output for the work entry has been successfully
  published.

Publication failures leave work entries `mapped`, with details in `message`.
Retry publication using saved output without mapping again. Keep updates for the
same item or Handle in order so an older update cannot overwrite a newer one.

The local database transaction does not include JSONL files or the external
Handle server. This design does not promise exactly-once remote HTTP execution
across crashes. Publication retries resume saved output using a durable per-Handle success
journal; explicit invalid retries use the conservative policy described below.

### Batch scheduling and future parallelism

Normal mapping selects `pending` work from the database for the selected
projects. Date filters remain available for explicit selections; dump files are
not the scheduling input.

Use project-specific batches with a configurable ceiling, initially 2,000 work
entries per batch. Count event snapshots, including successive updates to the
same item, rather than distinct catalog items, Handles, or bytes. Keep all
Handle output for one work entry together. The ceiling does not require waiting
for a full batch; automatic-run thresholds and maximum wait times are separate
scheduling policies.

Execute sync batches sequentially, drawing an eligible project at each batch
boundary using its priority weight (0–255, default 100). Zero disables sync;
positive weights determine relative chances and allow consecutive turns.
Explicit map/publish commands retain round robin project order. Select work
in ascending `id` order within each project. Retry older unpublished batches
before publishing newer batches for that project. A publication failure blocks
later publication for that project while other eligible projects can continue.
Explicit invalid retries use additional stale-output checks; weighted
scheduling alone does not resolve stale retries.

Projects never write to the same Handle. This permits future parallel execution
across projects without cross-project Handle ordering. Defer parallel execution
until sequential selection, durable output, and recovery are implemented and
measured, but prepare the implementation boundaries now:

- Separate database selection and durable batch membership, execution of one
  project batch, and scheduling across projects. A batch executor receives a
  concrete batch rather than independently selecting pending work.
- Keep processing context and output ownership local to a project batch; avoid
  requiring shared mutable processor or counter state between future workers.
- Keep database transactions short, outside mapping and remote publication.
- Retain the existing single-run ownership protection initially. Before enabling
  parallel workers, define atomic batch claims and crash recovery so work cannot
  be selected twice; the current work-state columns alone are not a worker claim.

Future execution should use a bounded project-worker pool, with at most one
active batch per project initially, a separate database connection per worker,
and a global limit on concurrent Handle requests. Preserve ordering across
batches within each project. Worker claims, recovery metadata, and concurrency
settings are deferred; do not add speculative columns to the agreed schema now.

## Archive and error policy

Kafka messages are immutable inputs; corrections arrive as new events. Error
policy is separate from batching: either stop without acknowledging an event or
optionally record the failure, acknowledge it, and continue. Handling harvest
errors that prevent full STAC reconstruction is a separate policy decision.

Unknown-project handling is configured separately through
`catalog.ignore_unknown_projects`: `false` stops harvest by default; `true` requires a durable
coordinate-bearing `unknown_projects` dump before advancing the shared
checkpoint. Ignored events count as filtered in run/live statistics under their
project identifier, with a warning per newly ignored event. They create no
catalog/work rows and do not count as mapping errors in batch summaries.
Missing/conflicting identities and malformed envelopes still stop harvest.

JSONL dumps are the original event archive, not the pending work queue. Include
topic, partition, offset, key, and the original payload, preserving per-partition
order. Support Kafka and archived files as alternative inputs to the same flow,
including replay after Kafka retention has removed older events.

Rotate dumps by a configurable event count as well as by day. The first day's
harvest can contain the complete retained history and reach several GB. Numbered
parts must preserve deterministic replay order across rotation and restart.
Event-count rotation is implemented through `consumer.dump_max_events` (default
100,000). Current dumps do not yet attach Kafka coordinates.

## Retention and compaction

Keep work cleanup implemented but disabled by default (`batch.retention_days = 0`).
All work remains until a positive retention period is explicitly configured.
Manual or scheduled maintenance can then delete completed `published` work older
than that period, for example 30 days. Use the
time the entry entered `published` (`updated_at`), not its original creation
time. Retain pending, mapped-but-unpublished, and invalid work. Summaries must
already account for results before their work entries are deleted.

Deleting work does not delete catalog records or dump archives. SQLite normally
reuses freed space for new entries, although the database can still grow when
incoming data exceeds the space freed.

Never run `VACUUM` unsupervised. File shrinking is an explicit operation with
Piddi stopped, a database backup, and sufficient free disk space checked.

Completed output has an independent maintenance policy: `compress_outputs = true`
and `output_retention_days = 0` under `[maintenance]` enable gzip compression but
keep archives indefinitely by default. Only published archive directories with a
matching published DB summary are eligible. Keep manifests readable and preserve
original-byte checksums; update their receipt paths after compression. Ready
batches and their receipts stay unchanged. Receipt writers hold advisory locks;
maintenance and project reset share an output lock, allowing compression beside
normal harvest/sync. Standalone receipts wait until their last-write UTC day ends.

Optional positive output retention (e.g. 30 days) removes completed batch trees
and closed receipts by completion/last-write time. Deletion first renames a batch
to a private `.deleting-<batch-id>` path and persists the rename; subsequent runs
can finish interrupted removal even if its manifest is gone. DB summaries and
Kafka dumps survive this policy. Published-work retention remains independent.

### Future task: daily statistics rollup

Deferred, not implemented. Keep the current four-table design until measured
growth of batch summaries justifies a rollup. The future option should be
disabled by default and independent of published-work and run-history retention.

Add a fifth application table, `daily_stats`, inside the same `piddi.db`, rather
than a separate database. Retain recent batch summaries and all unfinished or
failed batches in `stats`. Older fully published batches become eligible only
after publication/archive recovery is complete. Invalid-only batches need an
explicit finalization policy before they can be included.

Implementation tasks:

- Let `piddi maintain`, including the scheduled maintenance service, own the
  rollup. Mapping and publication continue updating batch rows; heartbeats
  continue updating run rows. Support a dry run and configurable retention age.
- Aggregate eligible batches by project and UTC date. Preserve mapped/rejected
  item counts, full/patch event counts, prepared Handles, the alias subset,
  published Handles, and errors.
  Attribute mapping counts to `mapped_at` and publication counts to
  `published_at`, including batches spanning midnight. Define error-date
  attribution explicitly: existing batch totals do not retain individual failed
  publication attempt timestamps.
- Add daily aggregates and remove exactly the included batch summaries in one
  transaction, coordinated with the batch-worker lock. An interrupted or repeated
  maintenance run must neither lose results nor count them twice. Exclude run
  rows from the rollup and keep their existing cleanup policy.
- Make `top` combine rolled-up totals with remaining batch summaries, using one
  read-only database snapshot. Preserve the totals start date and last-publication
  information after detailed summaries disappear.
- Preserve minimal completion information so archived-batch recovery cannot
  recreate a rolled-up batch or republish its Handles. Keep the conservative
  invalid-retry guard aware of successful publication recorded in either table.
  Project reset must clear that project's batch and daily totals together.
- Verify cross-day batches, aliases, publication failures/retries, crash recovery,
  repeated rollup, concurrent monitoring, project isolation, reset, and unchanged
  totals after work/run cleanup.

Rollup reduces retained detail and allows SQLite to reuse pages. It does not
automatically shrink the database file; physical compaction remains the separate
supervised operation described above.

## Implementation details and limits

The database lives at `<consumer.output_dir>/piddi.db`, or under
`catalog.directory`. It has the four application tables above; SQLite also
maintains its internal `sqlite_sequence` table to prevent work ID reuse after
retention. `stac_blob` contains a zlib-compressed snapshot envelope with the
unchanged full STAC item, collection/version identity, publication timestamp,
and optional local recovery provenance. This retains source fields without
adding database columns or mixing local provenance into the source item.

`map` prepares pending snapshots and seals batches without publishing. With no
date selector it processes all pending work; date selectors filter local
`created_at` UTC days. `publish` without paths publishes complete queued batches.
Explicit file publication remains available for operator-managed files but does
not acknowledge database work. Use queued publication for the work stream.

Batch manifests record exact work IDs and mapping outcomes. After files and their
directories are durable, one transaction associates successful work with the
batch, marks rejected work invalid, and inserts its mapping/error summary.
Recovery repeats this transaction idempotently. Successful publication advances
mapped work and updates the same summary row in one transaction, also
idempotently. Publication failure records a failed batch attempt and leaves work
mapped; a failed-attempt summary is not a unique-job count. HTTP may repeat after
a crash, but a repeated successful acknowledgement does not repeat the summary.

Each ready batch has an append-only `publication-progress.jsonl` journal tied to
its output checksum. Successful Handle writes are fsynced before any later write
to the same Handle. Recovery skips already completed positions, so replay cannot
roll a Handle back to an earlier successful snapshot. An unsuccessful write blocks
later writes to that Handle for that attempt, including across input chunks.
A torn final journal line is removed on recovery; a corrupt complete line blocks
publication. The most recent unrecorded remote write may still repeat after a crash.

Manual mapping filters may prepare newer work first. Publication waits for older
pending work, and sync maps the older prefix before publishing ready batches in
work-ID order. All workers use one nonblocking lock beside the database, including
map, queued publish, sync, retry, reset, and work cleanup.

Invalid retries are conservative: finish the original saved batch first. An
explicit retry is refused if newer work was mapped/published or the project has
any recorded successful publication. This avoids stale retries even after work
retention. Harvest a correction as a new event, or use an explicit whole-project
reset to replay all retained snapshots in order. Reset does not recover work
already removed by retention.

Live heartbeats and recent speed samples are stored in `stats` rows marked `run`,
keyed by project and run ID. Concurrent commands update their own rows without
changing batch totals. Each run retains up to one hour of samples; maintenance
prunes expired completed run rows. `top` reads one database snapshot for live
activity, prepared/published Handle totals, and backlog. It does not inspect
output manifests or status files. Batch summaries survive work retention and
run-history cleanup. Reset deletes only the selected project's statistics.
Database schema version 4 and batch manifest version 3 require a fresh start;
no migration is provided. Harvest checkpoints are shared by topic/partition: a
harvest clean must include every stored project. Project processing reset
preserves checkpoints and only requeues that project's retained work.

This implementation supersedes the separate per-project database and DuckDB
proposals. Parallel project workers remain deferred.

# Proposed catalog and work-stream design

Agreed on 2026-11-01. This is a design for a future implementation pass, not a
description of the current database schema. The current batching fix remains in
place until this design is implemented.

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
| `state` | `pending`, `mapped`, `invalid`, or `published` |
| `created_at` | When the job was stored |
| `updated_at` | When its state last changed |
| `batch_id` | Saved Handle batch; empty before mapping |
| `message` | Processing note or error explanation; empty when unnecessary |

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

Daily summaries retained independently of completed work entries.

| Column | Purpose |
| --- | --- |
| `date` | Summary date |
| `project` | Project |
| `items` | Successfully mapped items |
| `handles` | Successfully published Handles |
| `errors` | Failed processing attempts |

Primary key: `(date, project)`. All dates and timestamps use UTC. Weekly and
monthly reports can sum daily rows; the table name does not prescribe a reporting
interval. Job totals and failed-attempt totals have distinct meanings. Repeated
summary or maintenance runs must not count the same result again; summary
updates and their database progress must be transaction-safe.

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
across crashes. Detailed partial-publication recovery and ordering during
explicit retries remain implementation decisions for the next pass.

## Archive and error policy

Kafka messages are immutable inputs; corrections arrive as new events. Error
policy is separate from batching: either stop without acknowledging an event or
optionally record the failure, acknowledge it, and continue. Handling harvest
errors that prevent full STAC reconstruction is a separate policy decision.

JSONL dumps are the original event archive, not the pending work queue. Include
topic, partition, offset, key, and the original payload, preserving per-partition
order. Support Kafka and archived files as alternative inputs to the same flow,
including replay after Kafka retention has removed older events.

Rotate dumps by a configurable event count as well as by day. The first day's
harvest can contain the complete retained history and reach several GB. Numbered
parts must preserve deterministic replay order across rotation and restart.
Current dumps do not yet attach Kafka coordinates or rotate by event count.

## Retention and compaction

Allow manual or scheduled maintenance to delete completed `published` work older
than a configurable retention period, initially discussed as 30 days. Use the
time the entry entered `published` (`updated_at`), not its original creation
time. Retain pending, mapped-but-unpublished, and invalid work. Summaries must
already account for results before their work entries are deleted.

Deleting work does not delete catalog records or dump archives. SQLite normally
reuses freed space for new entries, although the database can still grow when
incoming data exceeds the space freed.

Never run `VACUUM` unsupervised. File shrinking is an explicit operation with
Piddi stopped, a database backup, and sufficient free disk space checked.

## Implementation follow-up

Define migration from the existing per-project catalogs, batch manifests, and
monitoring database before replacing them. Preserve current data and recovery
paths. This design supersedes the older separate per-project catalog/work
database and DuckDB proposals for the next implementation pass.

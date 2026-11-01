# Recovery

The fresh-start format uses one `piddi.db` and saved JSONL batches. Existing
per-project databases and old manifests are not migrated. See
[Operations](operations.md) for command examples and paths.

## Harvest interruption

The catalog update, independent work snapshot, and shared Kafka checkpoint commit
together. On restart, already stored topic/partition offsets are skipped, including
non-idempotent patches. An unresolved event stops harvesting before its checkpoint
or broker acknowledgement advances.

Raw archives carry source coordinates and the original payload. Use repeatable
`harvest --archive FILE` inputs to replay them, including compressed numbered
parts. Shared Kafka checkpoints require a complete harvest rebuild across stored
projects; project-only harvest cleanup cannot safely rewind a shared partition.

## Mapping interruption

A building batch does not advance work. Retry normal map/sync; the selected
snapshots remain pending. Building artifacts may remain for inspection.

A sealed batch has durable Handle/failure files, checksums, and exact work-ID
outcomes. If the process stopped before database acknowledgement, the next batch
command verifies the files and repeats the acknowledgement transaction. Repeated
acknowledgement cannot repeat the batch totals. A checksum error or missing source
work blocks that batch; restore the matching database and output together.

Mapping validation failures become invalid work with an explanation, while
unprocessed snapshots stay pending. Partial Handle output for a rejected entry is
rolled back before sealing.

## Publication interruption

Failed publication leaves work mapped. Retry `publish` or `sync` using the saved
output. Older failed publication blocks newer batches in that project, while
other projects can continue. The same Handle stays ordered within each batch.

The saved `publication-progress.jsonl` journal records successful output positions
per Handle, tied to the immutable output checksum. Each success is durable before
a later write to that Handle starts. Retries skip completed positions and defer
later updates after a failure. A torn final journal line is repaired on restart.
A crash between remote HTTP success and its journal append can repeat that last
write; earlier completed Handle versions are not replayed. The local system does not promise exactly-once remote execution. Successful
work-state transitions and batch totals are nevertheless committed together and
are idempotent on recovery. If acknowledgement completed before archival, the
next run archives the batch without republishing it.

## Explicit retries and reset

`retry --project NAME ID...` requeues invalid snapshots only before the project has
recorded successful publication and when no newer work was mapped/published.
Finish its original saved batch first. Otherwise harvest a correction as a new
event or explicitly reset the whole project's retained processing history.

`reset --project NAME` previews an ordered project replay. With `--apply`, after
harvest and all batch commands are stopped, it updates work in place without
copying the database, archives project output under `reset-archives/`, clears
project summaries, and requeues retained work.
Catalog records and shared checkpoints remain intact. Retention-deleted work
requires original archive replay into a fresh database.

## Storage

Do not delete database journals or edit batch payloads to bypass recovery checks.
Back up SQLite using its backup API or with all processes stopped. Keep its output
batches and harvest control file with it. Maintenance never runs `VACUUM` and only
deletes work whose published-state timestamp is older than the retention limit.

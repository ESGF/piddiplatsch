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

## Rebuild from dump backups, then catch up with Kafka

Compressed raw dumps can rebuild the database independently of Kafka retention.
Keep a continuous archive from the desired starting point to the backup cutoff;
the cutoff may be several days old if Kafka still retains the following interval.
Preserve the original numbered filenames and source coordinates. Legacy dumps
containing only payloads cannot provide a reliable Kafka handoff.

Stop harvest, mapping and publication services. Use a fresh output/catalog
directory so this rebuild cannot reuse old batches or publication journals. For
example, use a recovery configuration with these overrides alongside the usual
Kafka, STAC and Handle settings:

```toml
[consumer]
output_dir = "/mnt/piddi-recovery"

[catalog]
directory = "/mnt/piddi-recovery"
```

Replay the saved dump directory first:

```bash
piddi --config recovery.toml admin recover --archive /backup/piddi/dump
```

This reads top-level `dump_messages_*.jsonl` and `.jsonl.gz` parts in filename
order, without connecting to Kafka, rewriting the backup, mapping or publishing.
Explicit files and repeated `--archive` inputs also work. Replay can be restarted:
durable checkpoints skip overlapping events before applying them, including PATCH
events. A malformed archive stops the command; repair/restore it before continuing.
Internal archive gaps cannot be inferred reliably from Kafka offset numbers, so
archive completeness is a backup requirement.

After replay succeeds, start normal harvest using the same recovery directory:

```bash
piddi --config recovery.toml harvest --idle-timeout 0
```

On each partition assignment, harvest reads the database's **next offset** and
explicitly starts Kafka there, regardless of the consumer group's committed
offset. For example, a dump ending at offset 12345 resumes at 12346. The same
rule applies to ordinary restarts and rebalances, using fresh database progress.
The selected positions and retained ranges are logged. A partition absent from
the backup starts at its earliest retained offset; earlier lost history for such
a partition cannot be detected automatically.

Before assigning any partitions, harvest checks their current Kafka watermarks:

- A required position below the earliest retained offset stops with the missing
  interval. Restore newer/missing dump parts into this database, then retry.
- A position beyond Kafka's end stops with a topic/cluster/backup mismatch error.
- A position equal to Kafka's end is valid: harvest waits for new messages.

Automatic offset reset is disabled for catalog harvesting, so retention passing
the selected offset during fetching also stops consumption instead of silently
skipping ahead. Kafka cannot fill an interval missing from both Kafka and dumps.

Once the catalog and work queue are restored, resume normal mapping/publication
against the recovery directory. Dump recovery reconstructs pending work; it does
not restore old publication receipts or suppress remote Handle writes that were
already completed before the failure. Restore a matching database/output backup
instead when preserving that publication progress is required.

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

Manual reset/rebuild and catalog recovery commands live under `piddi admin`.
Normal harvesting, mapping, publication and scheduled maintenance keep their
regular commands. Dump replay also remains available through `harvest --archive`.

`retry --project NAME ID...` requeues invalid snapshots only before the project has
recorded successful publication and when no newer work was mapped/published.
Finish its original saved batch first. Otherwise harvest a correction as a new
event or explicitly reset the whole project's retained processing history.

`admin reset --project NAME` previews an ordered project replay. With `--apply`, after
harvest and all batch commands are stopped, it updates work in place without
copying the database, archives project output under `reset-archives/`, clears
project summaries, and requeues retained work.
Catalog records and shared checkpoints remain intact. Retention-deleted work
requires original archive replay into a fresh database.

## Storage

### Manual work-history rebuild

Use this occasional manual operation to replace retained event history in SQLite
with one full, published snapshot per current catalog item (including each
retained dataset version). It requires all retained work to be published:
pending, mapped, or invalid events must be processed/resolved first. Ready or
failed-publication batches also block consolidation. It does not enable
automatic retention or change its settings.

```bash
piddi admin rebuild                 # preview
make stop
piddi admin rebuild --apply         # leave free pages available for reuse
# Or, instead of the preceding command:
piddi admin rebuild --apply --vacuum # also compact the database file
make start
```

The rebuild covers all projects. It keeps catalog blobs and real Kafka
checkpoints unchanged. New work is ordered by the catalog's local `last_update`,
then project, collection and item ID. It uses a unique synthetic source identity
and new work IDs; these are not Kafka offsets. All new entries are published full
snapshots, so normal processing does not map or publish them again. Existing
processing summaries and published Handle totals remain in the database.

Before replacing work, the command streams the complete old work and summaries
to `work-rebuild-archives/<id>/work_items.jsonl.gz` and `stats.jsonl.gz` under
`consumer.output_dir`. Every work-row field is retained; `stac_blob` holds the
original compressed bytes encoded as base64, as recorded in `rebuild.json`.
These are historical row exports, not Kafka dump envelopes accepted by
`harvest --archive`. Old batches and project receipt directories are moved into
the same archive. Keep/back up this directory if you want the old event history;
archiving reduces live database content, not necessarily total disk usage.

Stop harvest, batch/publication and maintenance processes before applying. The
command refuses an active harvest or batch worker and an empty catalog. Database
changes are transactional; failures restore moved output. An interrupted rebuild
blocks harvest and batch processing until `piddi admin rebuild --apply` is rerun
with the same configuration. A durable database receipt lets this finish a
committed rebuild without replaying the rebuild itself. If optional VACUUM fails,
the rebuilt work stays committed and the same recovery command retries finishing
it.

Exporting the archive and rewriting SQLite need temporary disk space. Without
`--vacuum`, SQLite retains free pages for reuse. Optional VACUUM also needs free
disk space (potentially twice the database size) and requires database readers to
be closed; see [SQLite's VACUUM documentation](https://sqlite.org/lang_vacuum.html).
The command does not delete old rebuild archives automatically.

Do not delete database journals or edit batch payloads to bypass recovery checks.
Back up SQLite using its backup API or with all processes stopped. Keep its output
batches and harvest control file with it. Maintenance never runs `VACUUM`.
Database work cleanup removes only published work older than its retention limit.
Completed batch JSONL and closed receipts are compressed by default; their
removal is a separate opt-in policy (`maintenance.output_retention_days = 0`
keeps them indefinitely). DB summaries and Kafka dumps survive output removal.

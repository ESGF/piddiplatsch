# Recovery

Failure files are inspection artifacts. There is no `retry` command, and normal
commands never consume failure or skipped records as inputs.

Decoded failures include the source STAC record, error description, and available
project/source coordinates. They retain the historical directory layout:

```text
outputs/<project>/failures/r0/failed_items_<date>.jsonl
outputs/<project>/skipped/skipped_items_<date>.jsonl
```

Unresolved projects use the global `failures/` or `skipped/` directories. Original
Kafka messages remain in `outputs/dump/dump_messages_<date>.jsonl`.

Catalog harvest stops immediately on an unresolved event without acknowledging it.
Missing PATCH bases trigger per-item remote recovery: fetch the complete current
item without applying the patch, then persist remote-refresh mode. Later events
for that item also fetch current state. Harvest startup refreshes marked items
without new Kafka events; map remains local. A failed fetch stops harvest without
advancing the event checkpoint. See [remote recovery](operations.md#remote-current-item-recovery).

Fix the cause and repeat `harvest`; checkpoints prevent previously stored patches
from being reapplied. To rebuild a catalog, use `--clean-project NAME` or
`--clean-catalog`, using retained Kafka publications and per-item remote recovery where available.
Recovery requires the item to remain available in remote STAC; it does not restore
missing historical versions. Remote lag can temporarily omit an update, so repeat
harvest after the service catches up before mapping.
There is currently no harvest option to skip an unresolved event.

Mapping stops at the configured `consumer.max_errors` threshold. Fix the problem
and repeat the normal command using its catalog selection:

```bash
piddi map --project cmip6 --date today-1
# Or select an inclusive window and a bounded portion of catalog items
piddi map --project cmip6 --from 2026-10-08 --to today-1 --offset 1000 --limit 1000
```

Map and publish share date selectors and default to `last`; use `--date all` for
full filling. Mapping appends Handles to files named for each item's UTC event day.
An item's assets produce several Handle records, so publication offsets count a
different unit from mapping offsets. Inspect prepared files before publication:

```bash
piddi publish --project cmip6 --date today-1
piddi publish --project cmip6 --from 2026-10-08 --to today-1
```

Publication keeps its inputs and writes per-record receipts. Repeating a completed
input uses Handle overwrite semantics. `publish --retries` remains available for
transient HTTP failures within a run; it is independent of diagnostic files.
See [Operations](operations.md) for selection, ordering, and receipt details.

## Scheduled batch recovery

The worker's source remains the catalog. Failed mapping staging directories under
`batches/<project>/.building-*` are inspection artifacts; pending catalog changes
remain eligible for a future tick. Batches only become ready when mapping succeeds.

Ready batches are immutable. Publication failures retain their ready state and
receipts; repeating `piddi batch` republishes the same batch before newer work for
that project. A checksum mismatch or catalog generation mismatch blocks publication
for inspection. Successful batches are archived under `published/`. A crash after
remote writes but before recording success may repeat those writes with overwrite
semantics. Back up both the catalog and batch spool: mapping progress can reference
sealed batches whose publication has not completed.

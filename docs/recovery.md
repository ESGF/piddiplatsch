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
Fix the cause and repeat `harvest`; checkpoints prevent previously stored patches
from being reapplied. To rebuild a catalog, use `--clean-project NAME` or
`--clean-catalog`, provided Kafka still retains the necessary original publications.
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

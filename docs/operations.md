# Operations

For installation and manual testing, start with the [Quick guide](quick-guide.md).
For routine checks, use [Admin](admin.md).

## Manual processing

Run from the same working directory with the same configuration for every step:

```bash
piddi harvest
piddi map --project cmip6 --limit 100
piddi publish --project cmip6 --workers 4 --retries 3
```

Harvest stores incoming events; `--idle-timeout 0` keeps it running continuously.
Use one harvest writer per database. Map works offline and prepares pending event
snapshots. Its default total limit is 2,000 events across selected projects.
Publish sends saved batches to the configured Handle server and resumes committed
progress after interruption.

Use repeated `--project NAME` options to select projects, or `map --all-projects`.
Run `piddi COMMAND --help` for additional options.

## Automatic processing

```bash
piddi sync
piddi sync --force-map
piddi sync --project cmip6
```

Sync maps eligible work and publishes queued batches. `--force-map` skips the
count/time waiting thresholds. Priority-zero projects remain disabled.
The [Ansible deployment](deployment.md) runs sync on a timer.
**Both publish and sync write real Handles.**

## Files

Local runs use `outputs/`; standard deployments use `/var/lib/piddi/`.

| Path under that directory | Contents |
| --- | --- |
| `piddi.db` | Catalog, event snapshots, queued Handles, publication progress, and statistics. |
| `dump/` | Original Kafka events with topic, partition, and offset. |
| `batches/<project>/ready/<batch-id>/` | Mapping exports: `handles.jsonl`, `failures.jsonl`, and `manifest.json`. |
| `batches/<project>/published/YYYY/MM/DD/` | Completed batch exports. |
| `published/YYYY/MM/DD/` | Publication receipts. |

Inspect mapping output before manual publication. The database owns queued
payloads and progress; exports are audit copies after commit. Failed publication
blocks newer batches for that project. Fix the cause and repeat publish or sync.
Invalid mapping events need a correction or maintainer review; repeating map does
not retry them. See [Recovery](recovery.md).

## Maintenance

Ansible schedules daily maintenance when harvest is enabled. It compresses closed
files and applies configured retention; it does not publish Handles.

```bash
piddi maintain --dry-run
```

Review the plan before changing [retention settings](configuration.md#retention).

To remove duplicate Kafka events from closed dumps, including gzip archives:

```bash
piddi maintain --deduplicate-dumps --dry-run
piddi maintain --deduplicate-dumps
```

Deduplication keeps the first identical event across dump files in filename order.
It requires valid Kafka topic, partition, and offset coordinates and compares the
entire record, so conflicting payloads and legacy records without coordinates are
retained. It does not change Kafka checkpoints, catalog data, or failure logs.
Today's UTC dump files are skipped; they become eligible the following day.

To include deduplication in the daily maintenance timer, enable:

```toml
[maintenance]
deduplicate_dumps = true
```

The first scan reads all closed dumps. Later runs reuse checksummed fingerprints
in `dump/.deduplicate-cache/` for unchanged files, avoiding decompression and JSON
parsing. Changed files and invalid caches are rescanned; files are rewritten only
when duplicates need removal. Progress identifies each file and reports every
10,000 scanned events. Dry runs reuse existing caches but do not populate them.

The cache is disposable: deleting it forces a full scan. Maintenance also uses
temporary disk space for its index and one replacement file. Interrupted runs
can be repeated safely. Deduplication is disabled by default;
`--no-deduplicate-dumps` overrides the setting for one run.

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

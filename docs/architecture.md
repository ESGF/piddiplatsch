# Architecture

Piddi turns ESGF publication events into persistent identifiers. It supports
CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6 through project plugins.

## Processing stages

```text
Kafka or archived JSONL -> raw archive + piddi.db       harvest
pending work snapshots -> immutable Handle batches    map
saved batches -> Handle service + work progress       publish
pending work -> project batches -> publication        sync
```

Harvest maintains one SQLite database with four application tables:
`catalog_items`, `work_items`, `kafka_checkpoints`, and `stats`. It archives each
Kafka event before committing catalog updates, an independent full work snapshot,
and its shared topic/partition checkpoint together. Repeated events are skipped;
new events remain independent even when their payloads match.

Map reads pending snapshots and writes dataset/file Handles to project batches.
It stays offline and validates the fields and Handle records it needs rather
than the entire STAC schema. Publication retries use the saved files. Sync runs
one project batch at a time, round robin, capped at 2,000 work entries by default.
Projects never write to the same Handle; parallel project execution is deferred.

See the [storage and work-stream design](work-stream-design.md) for schema,
state transitions, ordering, summary durability, and recovery limits.

## Project routing

The router reads project identity from these fields, in order:

1. `data.payload.collection_id`
2. `data.payload.item.collection`
3. `data.payload.item.properties.project`

Conflicting identities are rejected. `collection_id` works for full publications
and patches. The plugin registry rejects duplicate names and overlapping project
identifiers at startup.

Harvest stores every registered project it encounters. Unknown projects stop
harvest by default. `catalog.ignore_unknown_projects = true` instead durably archives
them separately, counts them as filtered, and advances the shared checkpoint
without catalog/work rows. Invalid events and missing/conflicting project
identities still stop harvest without acknowledging the failed event.

Mapping uses `consumer.projects`, repeated `--project` options, or
`--all-projects`. Sync uses the configured selection and can restrict it with
`--project`. Changing the mapping selection does not change Kafka offsets.
`all` means registered plugins, not unknown project names.

## Project plugins

Built-in plugins share the mapping code and Handle models. Each plugin declares
its name, accepted project identifiers, PID fields, and processor factory in a
`PluginSpec`. Plugins are registered explicitly in the repository; there is no
external package discovery.

The shared classes are `StacProjectProcessor`, `ProjectDatasetRecord`,
`ProjectFileRecord`, `DatasetHandleModel`, and `FileHandleModel`. Plugins add only
project differences, such as PID field names and version metadata.

| Project | Source dataset PID | Source file tracking ID |
| --- | --- | --- |
| CMIP6 | `cmip6:pid` | `cmip6:tracking_id` |
| CMIP6Plus | `cmip6plus:pid` | `cmip6plus:tracking_id` |
| CMIP7 | `cmip7:pid` | `cmip7:tracking_id` |
| CORDEX-CMIP6 | `cordex-cmip6:pid` | `cordex-cmip6:tracking_id` |

CMIP6 also accepts known older field names without the namespace. Relationships
use resolved PIDs so dataset and file links stay consistent.

## Catalogs and recovery

SQLite stores compressed STAC items and indexes item identity, dataset version,
and project-scoped dataset versions. Each transaction saves the catalog item,
work snapshot, and shared Kafka checkpoint together.
Local checkpoints prevent duplicate Kafka events from applying a patch twice.

If a patch has no local base, harvest fetches the current remote STAC item.
It does not apply the queued patch to that response. Recovered items stay marked
for remote refresh on later events and harvest startup. Map stays offline.
See [Remote current-item recovery](operations.md#remote-current-item-recovery).

Map and predecessor lookup share the same read-only catalog snapshot. Optional
version links use the nearest strictly older version in the same project,
collection, and dataset. Indexed queries avoid scanning item payloads.

## Output

Map and sync write batches under `outputs/batches/<project>/`. Each includes
`handles.jsonl`, `failures.jsonl`, and a manifest recording exact work IDs,
outcomes, settings, counts, and checksums. Failed publication keeps output ready.
Completed batches are archived under `published/YYYY/MM/DD/` within the project.

Raw events go to `outputs/dump/` before processing. Each archive record includes
source coordinates, key, and original payload. Date/count rotation preserves
replay order. Failure files are for inspection; replay uses original archives.

Batch summaries, live heartbeats, and recent rate samples live in the shared
database. `piddi top` reads only that database; output files remain publication artifacts.

## Kafka consumer groups

Catalog harvest uses `<kafka.group.id>.harvest`. A clean rebuild saves a new
group generation and replays retained history. Keep the catalogs and
`.harvest-state.json` together.

Use one harvest writer per catalog directory. Kafka divides partitions among
members of a group. Separate local catalogs sharing a group will each receive
only part of the topic. Multiple writers need coordinated storage and ownership.
Map and sync do not consume Kafka.

## Code layout

| Module | Responsibility |
| --- | --- |
| `commands/` | CLI arguments, progress, and summaries. |
| `consumer.py` | Kafka polling, acknowledgements, signals, and shutdown. |
| `catalog/harvest.py` | Complete item state, patches, and catalog writes. |
| `catalog/api.py` | Catalog backend and path selection. |
| `runners/mapping.py` | Local snapshots, selection, and mapping input. |
| `core/pipeline.py` | Shared processing, counters, failures, and stop rules. |
| `runners/publish.py` | Prepared Handle publication. |
| `runners/sync.py` | Pending changes and batch mapping/publication. |
| `jsonl_stream.py` | Bounded file reading with source positions. |
| `testing/processing.py` | Fixture replay helpers. |

Runners close their input streams on completion, errors, limits, or interruption.
A caller that supplies a progress display keeps ownership of it. The `retry`
command requeues invalid work only when the conservative ordering checks allow it.

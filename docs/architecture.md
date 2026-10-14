# Architecture

Piddiplatsch harvests the shared ESGF Kafka publication topic into per-project
catalogs. Mapping reads complete local items and writes prepared Handles.

```text
Kafka -> raw JSONL + project catalogs (harvest)
project catalogs -> daily or sealed batch Handle JSONL (map/batch)
prepared Handle JSONL -> Handle Service + receipts (publish/batch)
```

Harvest is the only continuous service. The scheduled batch worker maps due
projects, seals immutable batches, and publishes ready batches sequentially.
Manual map/publish commands remain available; the old consume command is removed.

## Command execution

CLI command classes handle arguments, progress display, and summaries. Execution
is separated by input source:

- `consumer.py` owns Kafka polling, preflight, signals, and run shutdown for
  `harvest`. It feeds decoded keyed records into the shared
  processing pipeline and closes the Kafka input on completion or early exit.
- `catalog/harvest.py` reconstructs complete STAC items in per-project catalogs.
  Harvest source messages carry topic/partition/offset and an acknowledgement
  callback separately from their raw payload. The pipeline acknowledges successful
  catalog writes only after their local transaction commits; unresolved events
  stop harvesting. SQLite provides compressed storage, indexed version queries,
  and atomic item/checkpoint writes through the shared catalog contracts.
  Missing PATCH bases fetch current remote state without applying the patch.
  Persistent per-item remote-refresh mode also covers later POST/PATCH events
  and startup refreshes. A partial index selects only marked entries; mapping
  remains offline.
  Broker acknowledgements batch explicit durable offsets by count/time and flush
  on shutdown or revocation, avoiding a network round trip for every record.
- `runners/mapping.py` opens read-only per-project catalog snapshots, applies the
  indexed event-day selection followed by item offset and limit, and sends complete items lazily to the pipeline.
  Processors use the same snapshots for optional predecessor queries and do not
  fetch or reapply patches. `catalog.api` owns backend and path selection.
- `core/pipeline.py` owns processor selection, record execution, counters,
  failure/skipped-record persistence, progress updates, and stop policies. Its
  `ProcessingPipeline` accepts an iterable directly and returns per-run results;
  `process_messages` also manages progress created for a mapping call.
  It does not import Kafka or file-input readers.
- `runners/publish.py` publishes already prepared Handles independently of the
  mapping pipeline, using the bounded `jsonl_stream.py` reader.

Input lifetimes belong to the runners: they close file/Kafka streams on limits,
errors, or interruption. A caller that injects a progress display retains
ownership of it. The shared pipeline does not install signal handlers or close
run-wide monitoring; those remain with the command or Kafka run lifecycle.
JSON fixture replay helpers live in `testing/processing.py`.

## Routing

The router extracts project identity without validating a project-specific STAC
schema. It uses these fields in order and rejects conflicts:

1. `data.payload.collection_id`
2. `data.payload.item.collection`
3. `data.payload.item.properties.project`

The envelope `collection_id` works for both POST and PATCH events. A record for
an unselected or unknown project is a successful filtered result: it is counted,
but it is not sent to a processor or written as a processing failure. A record
matching a selected plugin is processed exactly once. The registry rejects
duplicate plugin names and overlapping project identifiers at startup.

Project plugins are built-ins registered explicitly by `PluginSpec`. A spec owns
the canonical plugin name, accepted publication project identifiers, and the
processor factory. Import failures are not hidden.

The registry is intentionally static while plugins live in this repository.
`PluginSpec` is also the future packaging boundary: independently distributed
plugins could expose specs through a standard Python package entry-point group
loaded with `importlib.metadata`. That discovery mechanism is not implemented
until an external plugin actually exists. A hook framework such as `pluggy` is
only justified later if the plugin contract grows beyond metadata, construction,
preflight, and processing.

CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6 share `StacProjectProcessor`, narrow
STAC record adapters, and project-neutral Pydantic Handle output models. Input
publication records are not validated against a complete STAC schema. Core
checks only the envelope and the object/field shapes consumed by mapping;
Pydantic validates the Handle records produced by each plugin. Thin plugin
classes declare project PID fields and retain genuine differences, such as
version index metadata. Predecessor lookup during catalog mapping is shared by
all built-in project processors and uses `lookup/catalog.py` on the local catalog
index. The version lookup factory defaults to `catalog`, retains explicit `stac`
queries, and can reuse caller-owned snapshots. Remote full-item recovery remains
a separate harvest concern.

Canonical PID fields currently are `cmip6:pid` / `cmip6:tracking_id` and
`cmip7:pid` / `cmip7:tracking_id`, plus `cordex-cmip6:pid` /
`cordex-cmip6:tracking_id` and `cmip6plus:pid` /
`cmip6plus:tracking_id`. CMIP6 additionally accepts its known legacy unnamespaced
fields. Relationships are built from resolved source PIDs rather than
independently generated identifiers.

## Selection

`consumer.projects` selects a list of plugin names or `all`. Repeated
`--project NAME` options and `--all-projects` on `map` override
config for a run. The batch worker uses the configured selection. `harvest` intentionally has no project selection: it retains
every raw queue message and caches each registered project encountered, so a
different mapping selection can be applied later. Unknown or malformed events
stop catalog harvesting and remain unacknowledged; `--no-catalog` keeps the previous
raw-only behavior. Harvest uses plugin metadata without constructing processors.

Only selected plugins are constructed. Catalog mapping skips remote preflight;
direct consumption retains its existing preflight behavior.

## Output and dump scope

Generated JSONL Handle records are plugin-scoped at
`outputs/<plugin>/handles/handles_<date>.jsonl`; every outbox line also carries
the canonical plugin name in `project`. This prevents records from concurrent
project mappings being mixed and gives downstream publishers an explicit
routing hint. Catalog mapping names daily files using the UTC event day, so
map and publish can use matching date selectors. Failure files are diagnostic
artifacts; recovery repeats normal commands against their original inputs.
Legacy retry library helpers remain for compatibility, without a CLI command.

The raw dump remains a single pre-routing stream under `outputs/dump` and is
mandatory for queue-consuming commands.
Because it is written in consumption order, it preserves the Kafka sequence for
replay and audit. `pid.log` records selected plugins and the first occurrence of
each filtered project at INFO, aggregate filtered counts in statistics, and
individual filter decisions at DEBUG.

## Kafka consumer groups

Harvest uses `<kafka.group.id>.harvest`; clean rebuilds persist a new group
generation. Keep one harvest writer for a local catalog directory. Kafka shares
partitions among members of a group, so multiple harvest instances require
coordinated storage and update ownership rather than independent local caches.
Map and batch project selection never changes Kafka group membership or offsets.

## Current scope

All four project identifiers observed in the 2026-08-25 queue dump are
implemented: CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6. Unknown future project
identifiers are filtered even when `all` is selected because `all` means all
registered plugins, not every identifier that may appear on the topic.

# Architecture

Piddi turns ESGF publication events into persistent identifiers. It supports
CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6 through project plugins.

## Processing stages

```text
Kafka -> raw JSONL + local STAC catalogs       harvest
local catalogs -> daily Handle JSONL          map
Handle JSONL -> Handle service + receipts     publish
local catalogs -> fixed batches -> publish   sync
```

Harvest is the continuous service. It stores a raw copy of each message and
updates complete STAC items in one SQLite catalog per project. Full publications
replace an item's state; patches update its stored base. Older dataset versions
stay available for version links.

Map reads a local snapshot and prepares dataset and file Handles. It does not
contact Kafka, remote STAC, or a Handle service. It checks the fields it needs
from the STAC item and validates the resulting Handle records with Pydantic.
It does not validate the entire input against a complete STAC schema.

Publish sends prepared Handle records to the REST service and writes a receipt
for every result. It keeps the input files.

Sync combines mapping and publication for pending catalog changes. It saves
fixed batches before publishing and tracks which changes have been mapped.
A timer runs sync regularly; each run exits when its work is done.

## Project routing

The router reads project identity from these fields, in order:

1. `data.payload.collection_id`
2. `data.payload.item.collection`
3. `data.payload.item.properties.project`

Conflicting identities are rejected. `collection_id` works for full publications
and patches. The plugin registry rejects duplicate names and overlapping project
identifiers at startup.

Harvest stores every registered project it encounters. Unknown projects and
invalid events stop catalog harvest without acknowledging the failed event.

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
and event day. Each transaction saves the item and its Kafka checkpoint together.
Local checkpoints prevent duplicate Kafka events from applying a patch twice.

If a patch has no local base, harvest fetches the current remote STAC item.
It does not apply the queued patch to that response. Recovered items stay marked
for remote refresh on later events and harvest startup. Map stays offline.
See [Remote current-item recovery](operations.md#remote-current-item-recovery).

Map and predecessor lookup share the same read-only catalog snapshot. Optional
version links use the nearest strictly older version in the same project,
collection, and dataset. Indexed queries avoid scanning item payloads.

## Output

Manual mapping writes `outputs/<project>/handles/handles_<date>.jsonl`.
Dates follow the UTC event day, so map and publish can use matching selectors.
Each record includes the project name.

Sync writes fixed batches under `outputs/batches/<project>/`. Each batch includes
Handle JSONL and a manifest with settings, counts, and a checksum. Failed
publication leaves the batch ready for another attempt.

Raw Kafka messages go to the shared `outputs/dump/` directory before routing.
Failure files contain records and error details for inspection. Recovery repeats
normal commands; failure files are not command inputs.
See [Operations](operations.md) for paths, selection, and publication rules.

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
A caller that supplies a progress display keeps ownership of it. Legacy retry
library helpers remain for compatibility, but there is no retry CLI command.

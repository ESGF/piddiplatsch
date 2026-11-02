# Architecture

```text
Kafka -> harvest -> SQLite event snapshots -> map -> Handle outbox -> publish
                                                               -> Handle service
```

`sync` combines mapping and publication. Harvest can continue while sync runs.

## Storage and ordering

One shared `piddi.db` stores the current STAC catalog, independent event snapshots,
Kafka checkpoints, Handle outbox, and statistics for all projects.

Harvest archives the original event, then commits the catalog update, work
snapshot, and checkpoint together. Stored Kafka offsets prevent duplicate events
from applying a patch twice. If a PATCH has no local base, harvest fetches the
current remote item without reapplying the patch. Recovered items refresh on
later events and harvest startup.

Map reads full local snapshots and commits immutable Handle payloads, work states,
and batch statistics together. It does not contact Kafka, STAC, or the Handle
service. JSONL files remain audit exports after commit.

Publish records successful writes in SQLite before advancing to a newer update
of the same Handle. No database transaction spans an HTTP request. A crash after
remote success but before local acknowledgement may repeat that write.
Older failed publication blocks newer batches for its project.

Sync processes one project batch at a time, selecting eligible projects by their
configured priority weights. Workers publish different Handles concurrently within
the current batch.

## Projects

Built-in plugins support CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6. They share
mapping logic and Handle models, with project-specific PID fields and metadata.
Registration is explicit; there is no external plugin discovery.

Harvest routes by collection/project identity and rejects conflicts. It stores
all registered projects; configuration selects which projects map and publish.
See [Contributing](https://github.com/ESGF/piddiplatsch/blob/main/CONTRIBUTING.md)
for adding a plugin.

`piddi top` reads the shared database for counters, heartbeats, batch activity,
and health. See [Admin](admin.md) for the monitoring guide.

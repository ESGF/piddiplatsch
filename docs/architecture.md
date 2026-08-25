# Architecture

Piddiplatsch consumes one shared ESGF Kafka publication topic and routes each
decoded event to zero or one selected project plugin.

```text
Kafka message
  -> JSON decode
  -> optional raw dump
  -> extract project identity
  -> selected project plugin
  -> project record mapping
  -> Handle backend
  -> result persistence and statistics
```

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
CMIP6 version lookup.

Canonical PID fields currently are `cmip6:pid` / `cmip6:tracking_id` and
`cmip7:pid` / `cmip7:tracking_id`, plus `cordex-cmip6:pid` /
`cordex-cmip6:tracking_id` and `cmip6plus:pid` /
`cmip6plus:tracking_id`. CMIP6 additionally accepts its known legacy unnamespaced
fields. Relationships are built from resolved source PIDs rather than
independently generated identifiers.

## Selection

`consumer.projects` selects a list of plugin names or `all`. Repeated
`piddi consume --project NAME` options and `--all-projects` override config for a
run.

Only selected plugins are constructed and preflighted.

## Kafka consumer groups

Kafka distributes each partition among members of one consumer group before
piddi performs project filtering. Therefore:

- one process may select several projects under one group;
- separate project-specific processes must use distinct `group.id` values;
- changing the projects associated with an existing group changes which records
  are considered complete and requires an explicit offset/replay decision.

Sharing a group between separate CMIP6 and CMIP7 processes is unsafe: either
process may receive and filter records intended for the other.

## Current scope

All four project identifiers observed in the 2026-08-25 queue dump are
implemented: CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6. Unknown future project
identifiers are filtered even when `all` is selected because `all` means all
registered plugins, not every identifier that may appear on the topic.

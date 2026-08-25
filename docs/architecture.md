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

CMIP6 is the only implemented project plugin. The shared queue also contains
other project identifiers; until their plugins are implemented, those records
are filtered even when `all` is selected because `all` means all registered
plugins, not every identifier present on the topic.

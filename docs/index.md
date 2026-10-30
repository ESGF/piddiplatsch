# Piddiplatsch

Piddiplatsch, or Piddi, reads ESGF STAC publication events from Kafka and registers
persistent identifiers with the Handle System. It supports CMIP6, CMIP6Plus,
CMIP7, and CORDEX-CMIP6.

The workflow has three steps:

1. **Harvest** stores events and builds local STAC catalogs.
2. **Map** turns catalog items into validated Handle records.
3. **Publish** sends those records to the Handle service.

The `sync` command maps and publishes pending changes. A timer can run it
regularly while harvest runs continuously.

## Guides

- [Quick guide](quick-guide.md): install Piddi and run the main commands.
- [Configuration](configuration.md): set connections, projects, and profiles.
- [Operations](operations.md): select records, publish, and check progress.
- [Deployment](deployment.md):
  install on Linux, run updates, and monitor services.
- [Recovery](recovery.md): inspect failures and restart work.
- [Architecture](architecture.md): understand the stages, plugins, and code.
- [Talks](talks.md): view the overview slides.
- [Building the documentation](building.md): preview and publish these guides.

## About the project

Piddi is developed for the ESGF community and used at DKRZ. Contributions from
other sites are welcome.

The name comes from the TV puppet
[Pittiplatsch](https://en.wikipedia.org/wiki/Pittiplatsch) and the abbreviation
PID: *curious by nature, persistent by design*.

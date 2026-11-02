# Piddiplatsch

Piddiplatsch (Piddi) reads ESGF STAC events from Kafka and registers persistent
identifiers with the Handle System for CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6.

1. **Harvest** stores events and current STAC records locally.
2. **Map** prepares Handle records from pending events.
3. **Publish** sends the prepared records to the Handle service.

For a deployed service, harvest runs continuously and a timer runs `sync` to map
and publish automatically.

- [Quick guide](quick-guide.md): clone, configure, and test manually.
- [Admin](admin.md): check progress and logs, and report errors.
- [Deployment](deployment.md): install and update with Ansible.
- [Configuration](configuration.md): connections, projects, and common settings.
- [Operations](operations.md): manual commands and output files.
- [Recovery](recovery.md): guidance for the service maintainer.
- [Architecture](architecture.md): a short technical overview.
- [Talks](talks.md): overview slides.
- [Building the documentation](building.md): preview these guides.

Piddi is developed for the ESGF community and used at DKRZ. Its name comes from
[Pittiplatsch](https://en.wikipedia.org/wiki/Pittiplatsch) and PID:
*curious by nature, persistent by design*.

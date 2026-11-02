# Piddiplatsch

Piddiplatsch reads ESGF STAC publications from Kafka and registers persistent
identifiers with the Handle System for CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6.

For a Linux service, follow the
[deployment guide](https://esgf.github.io/piddiplatsch/deployment/).
It covers first setup, updates, and monitoring. The steps below are for local use.

## Install and configure

```bash
git clone https://github.com/ESGF/piddiplatsch.git
cd piddiplatsch
conda env create
conda activate piddi
make install
cp etc/esgf-example.toml custom.toml
```

Edit `custom.toml` with your projects, Kafka connection, and Handle service
credentials. Then validate:

```bash
piddi config validate
```

## Harvest, map, and publish

Fill or update the shared `outputs/piddi.db`:

```bash
piddi harvest
```

Prepare the SQLite Handle outbox, inspect its exported files, then publish:

```bash
piddi map --project cmip6
# Inspect outputs/batches/cmip6/ready/<batch-id>/handles.jsonl.
piddi publish --project cmip6 --workers 4 --retries 3
```

## Map and publish with sync

Use `sync` to map and publish pending changes for the configured projects:

```bash
piddi sync --workers 4
```

To process pending changes now, without waiting for enough items or elapsed time:

```bash
piddi sync --force-map --workers 4
```

To run only one configured project:

```bash
piddi sync --project cmip6 --workers 4 --retries 3
```

The database owns queued Handle payloads and publication progress. A restart
resumes unpublished rows; exported batch files are not required after their
database commit. Schema 4 upgrades add the outbox automatically and import ready
manifest-v3 batches and their progress when processed. Keep legacy batch files
until imported. Older per-project catalogs and incompatible formats still
require a fresh start; see the recovery guide.

## Check progress and options

In another terminal:

```bash
piddi top
```

```bash
piddi --help
piddi sync --help
```

See the [configuration](https://esgf.github.io/piddiplatsch/configuration/),
[operations](https://esgf.github.io/piddiplatsch/operations/),
[recovery](https://esgf.github.io/piddiplatsch/recovery/), and
[deployment](https://esgf.github.io/piddiplatsch/deployment/)
guides for advanced use. See [Contributing](https://github.com/ESGF/piddiplatsch/blob/main/CONTRIBUTING.md)
for development and testing.

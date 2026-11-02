# Piddiplatsch

Piddiplatsch reads ESGF STAC publications from Kafka and registers persistent
identifiers with the Handle System for CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6.

For a Linux service, follow the
[Ansible deployment guide](https://esgf.github.io/piddiplatsch/deployment/).
The steps below are for manual testing.

## Install and configure

```bash
git clone https://github.com/ESGF/piddiplatsch.git
cd piddiplatsch
conda env create
conda activate piddi
make install
cp etc/esgf-example.toml custom.toml
```

Edit `custom.toml` with your Kafka connection, projects, and test Handle service
credentials. Check the settings:

```bash
piddi config validate
```

## Harvest, map, and publish

Harvest events into `outputs/piddi.db`. Press Ctrl+C when you have enough test
input, or let harvest finish after its idle timeout:

```bash
piddi harvest
```

Map pending events without publishing:

```bash
piddi map --project cmip6
```

Inspect `outputs/batches/cmip6/ready/<batch-id>/handles.jsonl` and
`failures.jsonl`. Map processes up to 2,000 events by default; use `--limit N`
to change that total.

Publish the prepared Handles to the configured server:

```bash
piddi publish --project cmip6 --workers 4 --retries 3
```

**Publish writes real Handles.** Use a test server and prefix for testing.
Repeat map and publish to process more pending events.

## Check progress and options

In another terminal, with the same environment and working directory:

```bash
piddi top
```

```bash
piddi --help
piddi harvest --help
piddi map --help
piddi publish --help
```

See [Configuration](https://esgf.github.io/piddiplatsch/configuration/) for settings,
[Admin](https://esgf.github.io/piddiplatsch/admin/) for routine monitoring, and
[Contributing](https://github.com/ESGF/piddiplatsch/blob/main/CONTRIBUTING.md)
for development and tests.

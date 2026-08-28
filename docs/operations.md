# Operations

## Runtime output

Piddiplatsch writes daily JSONL files beneath `consumer.output_dir`:

| Directory | Contents |
| --- | --- |
| `dump/` | Original Kafka messages written by every `harvest` or `consume` run. |
| `<plugin>/handles/` | Handle records produced by that plugin. Direct REST/pyhandle publication writes the JSONL record before contacting the server; each line also includes `project`. |
| `published/` | One run-scoped JSONL receipt per `publish` run, containing every successful or failed Handle outcome. |
| `skipped/` | Records deferred after transient external failures. |
| `failures/r<N>/` | Records that failed processing, grouped by retry count. |

The raw dump is intentionally global and always written before routing. It preserves
the consumed Kafka order and remains suitable for replay or investigation;
creating filtered per-plugin dumps would lose that simple ordering guarantee.

JSONL Handle output is always enabled, including direct publication. If the
audit record cannot be appended, that Handle is not sent to the server. A
server-side failure leaves the JSONL record available for inspection or later
publication.

Deferred publication writes each outcome immediately to a readable, unique
`published/published_<project>_handles_YYYY-MM-DD_HH-MM-SS.jsonl` file using UTC
and prints its path in the final CLI summary. Generic mixed-project batches use
`published_handles_...jsonl`. If another run starts during the same second,
`_2`, `_3`, and so on are appended. Parallel completion order may differ from
input order; `position`, `batch_index`, `source_file`, and `source_line` provide
stable ordering and provenance.

## Staged commands

```bash
# Kafka -> raw JSONL only
piddi --config custom.toml harvest

# raw JSONL -> project-scoped Handle JSONL only
piddi --config custom.toml map outputs/dump/dump_messages_2026-08-27.jsonl

# Handle JSONL -> REST Handle Service
piddi --config custom.toml publish --project cmip6 \
  outputs/cmip6/handles/handles_2026-08-27.jsonl

# Kafka -> raw JSONL -> Handle JSONL (default production ingestion)
piddi --config custom.toml consume

# All three stages in one process
piddi --config custom.toml consume --publish
```

`map` accepts files or directories plus `--project`, `--all-projects`,
`--limit`, `--offset`, and `--force`. It never contacts Kafka or a Handle
Service and does not modify its input dumps.

`publish --project NAME` validates every selected Handle before publishing and
stops without sending anything if a project is missing or different. Plain
`publish` remains the generic mode and permits mixed-project batches. The final
summary always provides per-project counts when project metadata is available.

## Real Handle service contract test

The opt-in live tests create uniquely named Handles and do not delete them, so
use a disposable test prefix. Configure the service without storing credentials
in the repository:

```bash
export PIDDI_LIVE_HANDLE_SERVER_URL=https://handle-test.example/api-root
export PIDDI_LIVE_HANDLE_PREFIX=21.TEST
export PIDDI_LIVE_HANDLE_USERNAME='300:21.TEST/testuser'
export PIDDI_LIVE_HANDLE_PASSWORD='...'
pytest -m live tests/live/test_real_handle_service.py
```

Set `PIDDI_LIVE_HANDLE_VERIFY_HTTPS=false` only for a trusted test service with
a self-signed certificate. The live suite checks create, overwrite/update,
read-back, bounded parallel publication, and same-PID update ordering.

The Docker mock adds a 50 ms delay to each valid PUT, approximating a serial
rate of 20 Handle registrations per second. Override it when starting Docker if
you need a different latency:

```bash
PIDDI_MOCK_HANDLE_PUT_DELAY_SECONDS=0.1 docker compose up -d
```

The delay occurs outside the mock store lock, so concurrent publisher workers
can overlap requests as they would with a real service connection/database
pool. GET requests and requests rejected before storage are not delayed.

These files, `pid.log`, and `stats.db` are ignored by Git and preserved by the
project's cleanup targets. There is no automatic retention policy. Dump and
dry-run files can grow quickly, so monitor disk usage and archive or remove old
files according to the site's operational policy.

## Safe inspection and retry

Inspect retry input before processing it and keep the original until the result
is verified:

```bash
piddi --config custom.toml -v retry outputs/failures/r0 --dry-run
```

`--delete-after` removes an input file only when all records succeed. Malformed
JSONL and skipped records are failures for this decision, so the source remains
available for inspection.

## Logging and statistics

The CLI writes to `pid.log` by default. Use `--log PATH` to select another file.
At INFO level it records the selected plugins, the first occurrence of every
filtered project identity, and periodic aggregate filtered counts. Per-message
filter decisions are available with `--debug` without flooding normal logs.
The optional SQLite reporter is controlled by `[stats]`.

## Shutdown behavior

SIGINT and keyboard interruption close the Kafka consumer, progress display,
and statistics reporters. Processing stops with a non-zero status after the
configured error limit or a fail-fast transient external failure.

## Local cleanup

`make clean` removes build, bytecode, and test artifacts only. `make clean-dist`
also removes other ignored development artifacts, but explicitly preserves
runtime output, logs, databases, local configuration, virtual environments, and
editor settings.

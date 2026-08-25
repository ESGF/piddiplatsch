# Operations

## Runtime output

Piddiplatsch writes daily JSONL files beneath `consumer.output_dir`:

| Directory | Contents |
| --- | --- |
| `dump/` | Original Kafka messages when `--dump` is enabled. |
| `<plugin>/handles/` | Handle records produced by that plugin using `--dry-run` or the JSONL backend. Each line also includes `project`. |
| `skipped/` | Records deferred after transient external failures. |
| `failures/r<N>/` | Records that failed processing, grouped by retry count. |

The raw dump is intentionally global and written before routing. It preserves
the consumed Kafka order and remains suitable for replay or investigation;
creating filtered per-plugin dumps would lose that simple ordering guarantee.

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

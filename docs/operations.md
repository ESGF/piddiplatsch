# Operations

## Runtime output

Piddiplatsch writes daily JSONL files beneath `consumer.output_dir`:

| Directory | Contents |
| --- | --- |
| `dump/` | Original Kafka messages written by every `harvest` run. |
| `catalog/` | One SQLite STAC catalog per registered project encountered by `harvest`, unless disabled. |
| `<plugin>/handles/` | Handle records produced by that plugin. Direct REST publication writes the JSONL record before contacting the server; each line also includes `project`. |
| `published/` | One run-scoped JSONL receipt per `publish` run, containing every successful or failed Handle outcome. |
| `<plugin>/skipped/` | Records deferred after transient external failures. |
| `<plugin>/failures/r<N>/` | Failed records with diagnostic metadata; the historical `r0` layout is retained. |
| `skipped/`, `failures/r<N>/` | Legacy or unresolved-project diagnostic records. |

The raw dump is intentionally global and always written before routing. It preserves
the consumed Kafka order and remains suitable for replay or investigation;
creating filtered per-plugin dumps would lose that simple ordering guarantee.

JSONL Handle output is always enabled, including direct publication. If the
audit record cannot be appended, that Handle is not sent to the server. A
server-side failure leaves the JSONL record available for inspection or later
publication.

Deferred publication writes each outcome immediately to a readable, unique
`published/published_<project>_handles_YYYY-MM-DD_HH-MM-SS.jsonl` file using UTC
and prints its path in the final CLI summary. If no project is known when the
receipt opens, the filename is `published_handles_...jsonl`. If another run starts during the same second,
`_2`, `_3`, and so on are appended. Parallel completion order may differ from
input order; `position`, `batch_index`, `source_file`, and `source_line` provide
stable ordering and provenance.

## Staged commands

```bash
# Kafka -> raw JSONL and project STAC catalogs (no Handle mapping)
piddi harvest --limit 100

# Keep the previous raw-dump-only behavior
piddi harvest --no-catalog --limit 100

# Clear all registered project catalogs and replay retained Kafka history
piddi harvest --clean-catalog

# Clear selected projects; other catalogs retain their state
piddi harvest --clean-project cmip6 --clean-project cmip7

# local catalog -> project-scoped Handle JSONL only
piddi map --project cmip6

# Add predecessor links using the local catalog index
piddi map --project cmip6 --version-history

# Handle JSONL -> REST Handle Service
piddi publish --project cmip6 --date 2026-08-27

# Scheduled workflow: map due projects and publish ready batches
piddi sync
```

`harvest --idle-timeout 0` keeps consuming continuously for a service. The normal
idle timeout is five seconds; an idle timeout does not override fail-fast errors.

`harvest --limit N` stops normally after dumping `N` messages, which is useful
for bounded tests against a live topic. By default it also maintains
`<consumer.output_dir>/catalog/<plugin>.sqlite` for every registered project it
encounters, regardless of `consumer.projects`. Catalogs open on first use;
harvest does not construct Handle processors or contact Handle services. Healthy
items are maintained locally; missing PATCH bases use remote current-item recovery
as described below.
Full publications replace an existing item's state; patches apply to its local
base. Older dataset versions remain available. Kafka coordinates are kept in
catalog checkpoints and failure metadata, without changing raw dump payloads.

Catalog harvesting uses `<kafka.group.id>.harvest`, starts at the earliest
retained offset when that group has no committed position, and disables automatic
offset storage/commits. After each catalog transaction succeeds, its Kafka offset
is queued for acknowledgement. Explicit durable offsets are committed
synchronously in batches, by default every 100 messages or 1 second, and flushed
on idle, shutdown, limits, and partition revocation. Polling a record alone never
makes its offset eligible for commit. Lost partitions are not committed.
Reruns resume from Kafka's committed position;
local checkpoints prevent already stored patches from being applied twice if
Kafka acknowledgement failed. A fresh group is not a guarantee of a complete
catalog: Kafka must still retain each full publication needed by later patches.

Unknown projects, invalid publications, failed remote recovery, and storage failures
stop catalog harvesting without acknowledging the failed message. Decoded failed
events are recorded in the existing failure files, including topic, partition,
and offset. Repair the cause and repeat `harvest`; the failed event remains
unacknowledged. Updates to an item must be ordered within one source
partition. Cross-partition ordering and force-refresh options are not implemented
yet. Do not delete a catalog and expect an already committed Kafka group to
reconstruct it automatically; use the clean options below.

`--clean-catalog` clears items and checkpoints for all registered projects;
repeatable `--clean-project NAME` limits clearing to those projects. SQLite files
are compacted to reclaim unused space, while raw dumps, Handle outputs, failure
records, and unrelated files remain unchanged. Projects never harvested do not
get empty databases. These options require catalogs enabled and cannot be combined
with each other.

Cleaning persists a new group generation under `catalog/.harvest-state.json`.
The resulting group is `<kafka.group.id>.harvest.<generation>`, so it replays from
the earliest retained Kafka history even if the previous group already committed
later offsets. Interrupted resets are completed before Kafka subscription on the
next run. After resetting, normal runs keep the generation and resume the rebuild;
specifying a clean option again intentionally starts another rebuild. Keep the
control file with the catalogs when moving or backing up their directory.

Project-scoped cleaning still replays the shared topic: other projects skip events
covered by their retained checkpoints. Replay appends raw dumps again, and
`--limit` counts all consumed messages, including skipped duplicates. Rebuilding
uses original publications retained in Kafka where available; missing patch bases
are recovered from remote STAC when possible. Run cleaning with other users of that catalog directory stopped;
concurrent clean/harvest commands are not coordinated.

### Remote current-item recovery

When a PATCH has no local base, harvest fetches the complete current item from the
project's configured `[stac].base_url` (or `[plugins.<name>.stac]` override), using
its configured timeout. Identity, collection, project, and item structure are
validated before writing. The triggering patch is **not applied** to the fetched
item. There is no whole-catalog bootstrap and no dummy-item fallback.

Recovered entries retain remote-refresh provenance outside the source STAC JSON:
endpoint, UTC fetch time, and original triggering Kafka coordinates. Subsequent
PATCH and POST events for those entries refresh from remote STAC instead of
modifying or replacing them with potentially older queued state. Checkpoints and
recovered state commit atomically. Duplicate Kafka offsets do not trigger another
fetch. Healthy entries continue using local patches without network calls.

On each harvest startup, marked entries are refreshed even without new Kafka
messages, using a partial SQLite index rather than scanning healthy entries. An
unavailable service, missing item, or invalid response stops harvest and preserves
the previous item/checkpoint. Markers remain until their project catalog is cleaned
and rebuilt; without a revision contract, a full queued publication cannot prove
it is newer than recovered remote state.

This mode follows current remote state; it does not prove the triggering patch
has been applied. A lagging remote service can temporarily omit changes. Repeat
harvest before mapping to refresh marked items after the service catches up. Map
always uses its local snapshot and never performs a remote refresh itself.
Catalog event-day selection retains the triggering/latest consumed event time;
`fetched_at` is recovery provenance, not a revision or freshness guarantee.

Configure `[catalog].enabled = false` or use `--no-catalog` for raw-only harvest;
that mode retains the existing Kafka group and acknowledgement behavior.
`[catalog].backend` currently supports `"sqlite"`; `[catalog].directory` overrides
the default catalog directory. `map` reads complete local catalog items exclusively;
raw dumps remain available for inspection.

Tune broker acknowledgement frequency with `[catalog].kafka_commit_batch_size`
and `[catalog].kafka_commit_interval_seconds`. Setting batch size to `1` restores
per-message synchronous broker commits, which can severely limit throughput when
broker latency is high. SQLite item/checkpoint transactions remain durable per
message regardless of broker batching. A crash can replay the unacknowledged batch;
local checkpoints skip the already stored events, including non-idempotent patches.

`map` accepts `--project`, `--all-projects`, `--catalog-dir`, and the same
`--date`, `--from`, `--to`, `--limit`, and `--offset` selectors as `publish`.
Without an explicit project selection it uses `consumer.projects`. Named selections
require every selected project's catalog to exist; `--all-projects` selects the
registered catalogs available in the directory. Mapping opens read-only snapshots
and performs no Kafka, remote STAC, or Handle Service calls. Patches were already
applied during harvest. Pending catalog cleans block mapping until harvest finishes
the reset.

Both commands default to `--date last`. Supported dates are `YYYY-MM-DD`, `today`,
`yesterday`, `today-N` (including `today-0`), `last`, and `all`. Relative selectors
use the local calendar. `last` means the greatest available day across selected
catalogs for map, or daily Handle files for publish. Use `--date all` for full
initial filling. Both commands accept inclusive `--from DATE --to DATE` ranges;
both boundaries are required and cannot be `last` or `all`. A range cannot be
combined with `--date`. Publish also accepts explicit Handle paths, which cannot
be combined with date selectors.

Catalog selection uses an indexed UTC event day: publication time, falling back
to STAC `updated`, then `created`. Naive timestamps are treated as UTC. Undated
items are included only by `--date all`. A selection reads the current item state
whose latest event falls in that window; it does not reconstruct historical state.
Map writes `handles_<event-day>.jsonl`, so the same date selector selects the
corresponding daily publication files. Undated items use the current UTC output
day. Handle record timestamps still describe the actual mapping run.

Map order follows selected project order, then event day, collection, and item ID.
Offsets and limits apply after date filtering and count catalog items; publish
counts Handle input records, so an item with several assets has several publication
records. Mapping and predecessor lookup share each project's snapshot. Concurrent
harvest writes are visible on the next map run. Repeating map appends prepared
Handles; it does not mark items as mapped. Mapping stops at `consumer.max_errors`
when configured; earlier successful output remains written.

Predecessor linking is disabled by default during initial filling. Enable
`[catalog].version_history = true` or pass `--version-history`; a per-run
`--no-version-history` overrides the setting. Every built-in project uses an
indexed search for the greatest version strictly below the current version within
the same collection and dataset, including versions outside the selected date
window. Mapping uses the `CatalogLookup` adapter with its item snapshot.
`[lookup].backend` defaults to `catalog`; selecting `stac` affects separate version
queries and does not enable network calls in map. The `consume` command has been removed; harvest owns remote item recovery.

SQLite schema version 5 retains the event-day index and adds remote-refresh
provenance with a partial index, and adds pending batch metadata. Run `harvest` once to upgrade
existing catalogs, even if no new Kafka events arrive. Migration derives dates
from retained timestamps without changing compressed payloads. Dates discarded by
older schemas cannot be recovered. Read-only legacy catalogs support unfiltered
library reads; CLI date selection requires this upgrade.

Failures preserve the complete STAC record and diagnostic metadata for inspection.
They are never used as command inputs. Fix the cause and repeat map using the
original catalog and date window, optionally with an offset and limit. There is
no `retry` command.

Migration: `map PATH` and `map --force` remain removed. Run harvest with catalog
mode enabled, then `map --project NAME --date all` for full initial filling.
`map --date` now selects catalog event days, rather than raw dump files.
`--catalog-dir` selects an alternate catalog directory.

## Scheduled immutable batches

`piddi sync` runs one worker tick: publish already ready batches, then map projects
that have reached `[batch].trigger_items` distinct pending catalog items or
`[batch].max_wait_seconds` since their first pending change. Defaults are 1,000 items
and one hour. `--force-map` ignores thresholds for nonempty pending selections.
Ready publication is independent of mapping thresholds and performs real Handle
writes. A shared filesystem lock prevents overlapping ticks.

By default, sync processes `[consumer].projects`. Repeat `--project NAME` to
restrict both ready-batch publication and new mapping to selected configured
projects. Unknown or unconfigured projects fail before publication begins.
`--workers N` and `--retries N` override `[batch].publish_workers` and
`[batch].publish_retries` for this run, including existing ready batches; retries
may be zero. For example: `piddi sync --project cmip6 --workers 4 --retries 0`.
Without overrides, scheduled runs keep using the TOML settings. Projects and
batches run sequentially; workers publish Handles concurrently within each batch.

Catalog changes use durable monotonic sequences and generation IDs, independent of
source event dates. Repeated changes coalesce by item and preserve its oldest
pending time. Each tick captures a sequence frontier per project and drains eligible
items in `[batch].max_items` chunks (default 1,000), taking turns across projects.
Changes arriving during the run remain pending for the next tick. Mapping and
predecessor lookup reuse a read-only snapshot. Every included asset must validate;
failed mapping output remains in a diagnostic staging directory and its catalog
items remain pending. Configured project selections apply; missing catalogs are
skipped until harvest creates them.

A successful run durably seals `handles.jsonl` and a versioned manifest in
`<output_dir>/batches/<project>/ready/<batch-id>/`. The manifest records catalog
generation, sequence bounds, project, profile, history option, counts, and checksum.
The mapping checkpoint advances only after sealing. A crash between sealing and
acknowledgement is recovered from the ready manifest without remapping. Payloads
are never appended after sealing and are verified before publication.

Publish runs immediately when a batch becomes ready. Failed publication leaves it
ready, preserving receipts and blocking newer work for that project. Later ticks
repeat that same batch with overwrite semantics; they never read diagnostic STAC
failure files. Other projects can continue. Successful batches move to `published/`
within the project's batch directory. A crash after remote writes but before the
published marker can cause repeated writes. This is at-least-once publication.

Schema version 5 adds bounded pending metadata; harvest upgrades existing catalogs
and queues their current items once. Manual `map --date ...` still appends daily
files and does not acknowledge the scheduled worker queue. Changing mapping
configuration alone does not dirty catalog items; use manual full mapping/publication
when intentionally rebuilding Handle content after a configuration change.
No retention policy is applied to batch artifacts. For timer deployment and enablement,
see [Deployment](https://github.com/ESGF/piddiplatsch/blob/main/deploy/README.md#scheduled-map-and-publish).

## Deferred publication

Publish completed files after mapping, using the project's configured Handle
profile. Profiles contain the REST server, prefix, and credentials; see
[Handle configuration](configuration.md). Keep credentials in the ignored site
configuration file.

```bash
# Publish yesterday's prepared CMIP6 Handles
piddi publish --project cmip6 --date yesterday

# Publish available daily files from October 8 through yesterday
piddi publish --project cmip6 --from 2026-10-08 --to today-1

# Try the first 1,000 records of a specific completed file
piddi publish --project cmip6 --date 2026-08-25 --limit 1000

# Continue with the next 1,000 records, allowing transient retries
piddi publish --project cmip6 --date 2026-08-25 \
  --offset 1000 --limit 1000 --retries 3
```

Explicit inputs can be one file, several files, or a directory. Offsets count
nonblank input lines across the selected files; limits cap attempted records,
including failures. Publication never changes or deletes its inputs. Handle
writes use overwrite semantics, so a completed, immutable input can be replayed
after interruption. The command exits non-zero if any record fails.

Use `--from DATE --to DATE` with `--project` to select an inclusive date range.
Both boundaries are required and accept `YYYY-MM-DD`, `today`, `yesterday`, or
`today-N`, using the local calendar date. Available daily Handle files are
published oldest first; days without files are skipped. An empty range fails
before publication. The start must be on or before the end; `last` and `all` are not
range boundaries. Range options cannot be combined with `--date` or explicit
paths. Offsets and limits apply across the whole range in one publication run.

`--retries` covers transient connection errors, timeouts, rate limiting, and
server errors. The delay starts at one second and doubles up to 60 seconds;
change its initial value with `--retry-delay`. Permanent client errors such as
invalid credentials are not retried. `--workers N` enables concurrent requests
for different Handles, while updates to the same Handle retain source order
within the run.

`publish --project NAME` validates each Handle immediately before sending it.
Missing or different projects are recorded as failures, while valid records
continue. Without `--project`, the first parsed record selects the service
configuration; subsequent records must match its project. Each JSONL file is
opened once and processed in batches of at most 256 records. Writes for the same
Handle stay in input order, including across batches. Malformed lines receive
failure receipts; earlier writes are not rolled back. No staging database or
preliminary scan is used, so progress has no known total and receipt
`batch_total` is `null`. Parent references are sent as provided; receipt metadata
comes only from the current record. The final summary provides per-project
counts and up to 100 error messages; receipts retain all failures.

Each receipt line records the outcome, action, PID, URL, project, dataset, asset,
source location, position, retry count, and error. Missing dataset or asset
metadata remains `null`. `batch_index` and `position` refer to the entire
selected input, independently of internal batch boundaries. Receipts are
written regardless of log verbosity. INFO logs cumulative publication progress
every 30 seconds as results arrive and a final summary, including project,
successes, failures, retries, elapsed time, and average processing rate.
The receipt path is logged when the file opens. These summaries cover each publication attempt, not unique
Handles across runs. Individual successful writes are logged at DEBUG; failures
remain at ERROR.

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

These files, `pid.log`, and `piddi.db` are ignored by Git and preserved by the
project's cleanup targets. There is no automatic retention policy. Dump and
Handle JSONL files can grow quickly, so monitor disk usage and archive or remove
old files according to the site's operational policy.

## Inspection and repetition

See [Recovery](recovery.md) for diagnostic file locations and restart behavior.
After fixing a mapping problem, repeat the normal command against the catalog:

```bash
piddi map --project cmip6 --date today-1 --offset 1000 --limit 1000
piddi publish --project cmip6 --date today-1
```

Mapping failures and skipped records are inspection artifacts. Commands do not
read them again or delete them. Repeating mapping appends to the selected daily
Handle files; publication preserves its input and uses overwrite semantics.
Inspect output and use an explicit date or completed file when repeating a batch.

## Terminal progress

Progress is displayed by default for `harvest`, `map`, `sync`, and `publish`.
Use the global `--silent` (or `-s`) or `--no-progress` option to hide it:

```bash
piddi --no-progress map --project cmip6
piddi -v publish --project cmip6 --date yesterday
```

Harvest shows processed events/rate, committed catalog writes, duplicate Kafka
replays, PATCH events, remote refreshes during event processing, errors, and elapsed
time. Duplicate, patch, and remote refresh counters appear when nonzero. These
are run counters, not the total number of distinct items in a catalog.
Raw-only harvest shows event counts without catalog or Handle fields. Idle polls
refresh elapsed time even when no messages arrive.

Map shows catalog items/rate and prepared Handle counts/rate. Filtered items,
warnings, retractions, replicas, and skipped records appear when nonzero. Errors
and elapsed time are always shown; error age appears after an error. Sync names the
project and phase, switching from mapping counters to a publication bar with the
sealed batch's known Handle count, rate, and ETA. Manual publication streams its
input without a preliminary count, so its total can remain unknown; its bar shows
completed records and absolute input position. `--silent` hides all phase bars.
Final command summaries remain visible.

Kafka's native messages use Python logging instead of writing directly over the
progress line. The existing logging level/destination controls their visibility;
console logging uses tqdm-safe writes. Healthy harvest defers remote HTTP imports
until a missing/recovered item needs a fetch. Local catalog mapping also leaves
HTTP clients and codecs unloaded; publication imports its selected remote backend.
Optional HTTP codecs can still issue
native-extension GIL warnings when actually loaded; Piddi does not suppress them
or force the GIL off.

## Logging and statistics

The CLI writes WARNING and above to `pid.log` by default. Use `-v` for INFO,
`-vv` or its memorable `--debug` alias for DEBUG, and `--log PATH` to override
the configured file. `--silent` remains an alias for hiding progress;
`--progress/--no-progress` provides the explicit form. At INFO level Piddiplatsch
records the selected plugins, the first occurrence of every filtered project
identity, publication summaries, and periodic aggregate counts. Successful
per-Handle writes and per-message filter decisions are available at DEBUG level.
INFO summaries have short operation labels: `[harvest]`, `[map]`, `[publish]`,
`[sync]`, `[sync/map]`, or `[sync/publish]`. They use grouped counts and rates with
units; harvest omits Handle counts. Full statistics dictionaries are logged at
DEBUG instead of INFO. SQLite reporting and publication receipts keep full detail.

The `[logging]` configuration provides the service defaults:

```toml
[logging]
level = "WARNING"
file = "/var/log/piddi/piddi.log"
```

File logging uses a watched handler: after logrotate renames the active file and
creates a replacement, Piddiplatsch switches to the new file on its next log
write. Full recovery and skipped details remain available in JSONL independently
of the selected logging level.
The optional SQLite reporter is controlled by `[stats]` and is opened by
`piddi harvest`, `piddi map`, and the mapping phase of `piddi sync`. Publication
does not yet report to this database. The read-only `top` command never updates it.

The database contains a versioned current-run status model. A heartbeat is
updated independently of Kafka traffic, and processing outcomes are split by
canonical project. Inspect it without contacting Kafka or the Handle service:

```console
# live view (Ctrl-C exits)
piddi top

# one project, one terminal snapshot
piddi top --project cmip7 --once

# machine-readable status
piddi top --json

# use a six-hour history window
piddi top --history 360
```

`top` marks unfinished runs stale when their heartbeat exceeds
`stats.stale_after_seconds`. An idle topic is healthy while the process keeps
heartbeating. The heartbeat interval is configured with
`stats.heartbeat_interval_seconds`. Both default to 5 and 15 seconds,
respectively. `top` is read-only and reports a clear error for a missing or
invalid monitoring database.

The database appends cumulative per-project samples every
`stats.sample_interval_seconds` (15 seconds by default), including samples at
run startup and shutdown. The history table shows counter changes, average
input throughput, Handle throughput, catalog writes, and a compact input
throughput trend for the last
`stats.history_minutes` (60 minutes by default). `--history MINUTES` overrides
that window. Samples older than `stats.sample_retention_days` (30 days by
default) are removed; set it to `0` to retain them indefinitely. Samples contain
counters only; raw messages and log lines are never stored.
Harvest input is labeled as events, while map/sync input is labeled as items.
Catalog writes count committed item writes, excluding duplicate Kafka replays.
`top --json` also includes catalog refresh counts. Monitoring schema 3 adds these
counters automatically when a writer opens an existing database; earlier history
is preserved, with zero for the previously unrecorded catalog counters. `top` can
also read schema 2 databases without migrating them.

## Shutdown behavior

SIGINT and keyboard interruption close the Kafka consumer, progress display,
and statistics reporters. Processing stops with a non-zero status after the
configured error limit or a fail-fast transient external failure.

## Local cleanup

`make clean` removes build, bytecode, and test artifacts only. `make clean-dist`
also removes other ignored development artifacts, but explicitly preserves
runtime output, logs, databases, local configuration, virtual environments, and
editor settings.

## Deployment

After Piddi is checked out and installed into its Conda environment manually,
the Ansible playbook configures one service per VM. It renders the site
configuration from the same `custom.toml` used by manual runs, adds defaults
for omitted production paths, and uses
`/etc/piddi/piddi.toml`, `/var/lib/piddi`, and `/var/log/piddi/piddi.log` and
configures systemd plus hourly log rotation. For the short production and
Vagrant procedures, see [deploy/README.md](https://github.com/ESGF/piddiplatsch/blob/main/deploy/README.md).

Mount an external data disk at `/var/lib/piddi`; the systemd service waits for a
configured mount. If several isolated Piddi installations share one VM, prefer
one Podman or Docker container per workflow.

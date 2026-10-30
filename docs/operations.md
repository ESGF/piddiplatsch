# Operations

Use `harvest` to store publications, `map` to prepare Handle records, and
`publish` to send them. Use `sync` to map and publish pending changes in one run.
For service setup, updates, and daily checks, see
[Deployment](deployment.md).

## Staged commands

```bash
# Read Kafka and update local STAC catalogs
piddi harvest --limit 100

# Prepare Handles from all available days for one project
piddi map --project cmip6 --date all

# Inspect the Handle files, then publish a chosen day
piddi publish --project cmip6 --date yesterday

# Map due changes and publish ready batches
piddi sync
```

Harvest exits after five seconds without messages. Use `--idle-timeout 0` to
keep it running. `--limit N` stops after N messages, including duplicate events.
Harvest stores all registered projects it encounters, regardless of the mapping
project selection. It does not contact the Handle service.

Full publications replace the stored item. Patches update its local base.
Older dataset versions stay in the catalog. Unknown projects, invalid events,
failed remote recovery, or storage errors stop harvest.

### Kafka offsets

Catalog harvest uses the group `<kafka.group.id>.harvest`. A new group starts
at the earliest retained Kafka offset. Piddi acknowledges events only after
saving them locally. It sends acknowledgements every 100 messages or one second
by default, and flushes them on idle, shutdown, limits, or partition revocation.

After a restart, local checkpoints skip events already saved. This prevents a
patch from being applied twice if a Kafka acknowledgement failed. A failed event
is left unacknowledged. Fix the cause, then run harvest again.

Updates to one item must arrive in order within one Kafka partition. Ordering
across partitions is not supported. Keep one harvest writer per catalog directory.

`harvest --no-catalog` saves raw messages only. It uses the original Kafka group
and configured acknowledgement behavior, rather than catalog harvest's rules.

### Rebuild catalogs

Stop other users of the catalog directory before rebuilding. A rebuild needs
publications still retained in Kafka. Missing patch bases may be recovered from
remote STAC, but missing historical versions cannot be restored that way.

```bash
# Rebuild one project
piddi harvest --clean-project cmip6

# Rebuild several projects
piddi harvest --clean-project cmip6 --clean-project cmip7

# Rebuild all registered projects
piddi harvest --clean-catalog
```

The clean options clear the selected catalogs and checkpoints, then compact the
SQLite files. Dumps, Handle files, and diagnostics are kept. The two forms cannot
be combined and require catalogs to be enabled.

A clean run saves a new Kafka group generation in `catalog/.harvest-state.json`.
This lets it replay retained history despite earlier committed offsets. Other
projects skip events covered by their existing checkpoints. Replayed events are
appended to the raw dump again.

If rebuilding stops, use normal `harvest` to continue. Repeating a clean option
starts another rebuild. Keep `.harvest-state.json` with the catalogs when moving
or backing them up. Do not delete a catalog and expect the existing Kafka group
to rebuild it automatically. Check ready sync batches before cleaning; see
[Recovery](recovery.md#scheduled-batch-recovery).

### Remote current-item recovery

When a patch has no local base, harvest fetches the complete current item from
the project's STAC endpoint. It checks the identity and item structure before
saving it. It does not apply the queued patch to the fetched item.

The recovered item is marked for remote refresh. Later events for that item
fetch current state again. Harvest also refreshes marked items at startup,
even if Kafka has no new messages. Healthy items still use local patches.
Duplicate Kafka events do not cause another fetch.

A failed or invalid fetch stops harvest and keeps the previous saved state.
There is no placeholder item. Refresh markers remain until the project catalog
is cleaned and rebuilt.

Remote STAC may lag behind Kafka. A fetched item may therefore lack the latest
change. Run harvest again after STAC catches up, before mapping. Recovery follows
current state; it does not restore missing historical versions or prove that a
particular patch has reached STAC. Map never refreshes remote items itself.

## Select items and dates

Map reads complete items from local catalogs. It does not contact Kafka, STAC,
or the Handle service.

```bash
piddi map --project cmip6 --date yesterday
piddi map --project cmip6 --project cmip7 --date all
piddi map --all-projects --date all
piddi map --project cmip6 --from 2026-10-08 --to yesterday
```

Without a project option, map uses `consumer.projects`. Named selections require
the selected catalogs to exist. `--all-projects` uses registered catalogs present
in the directory. Use `--catalog-dir PATH` to read another catalog directory.

Map and publish accept the same date selectors:

| Selector | Meaning |
| --- | --- |
| `YYYY-MM-DD` | One day |
| `today`, `yesterday`, `today-N` | A day relative to the local calendar |
| `last` | Latest available day; the default |
| `all` | All available days, including undated catalog items |
| `--from DATE --to DATE` | An inclusive range; both ends are required |

Do not combine a range with `--date`. Range ends cannot be `last` or `all`.
For map, `last` means the latest event day across the selected catalogs. For
publish, it means the latest dated Handle file.

Catalog event days use UTC publication time, falling back to STAC `updated`,
then `created`. Map selects the current item state whose latest event falls in
the window. It does not reconstruct historical item states. Undated items are
selected only by `all` and written to the current UTC day's Handle file.

Map orders items by project, event day, collection, then item ID. `--offset`
skips selected items and `--limit` caps items processed. Publish counts Handle
records instead. One catalog item can produce many Handle records.

Mapping appends to daily `handles_<event-day>.jsonl` files. Repeating it can
append the same Handles again. It does not clear the sync worker's pending queue.
Mapping stops at `consumer.max_errors`; output already written is kept.
Concurrent harvest changes become visible on the next map run.

### Link older dataset versions

Predecessor linking is disabled by default. Enable it for one run with:

```bash
piddi map --project cmip6 --date all --version-history
```

Piddi finds the nearest strictly older version in the same project, collection,
and dataset. It can use older versions outside the selected date window.
Set `catalog.version_history = true` for a permanent default. Use
`--no-version-history` to turn it off for one run.

### Upgrade older catalogs

Run harvest once to upgrade older catalogs to schema version 5. This adds the
metadata needed for date selection, remote refresh, and pending sync work.
The upgrade queues existing items once for sync. It derives event days from
stored timestamps; dates already lost by an older schema cannot be recovered.

The old `map PATH` and `map --force` forms are removed. Harvest into catalogs,
then use `map --project NAME --date all` for a full mapping run.

## Scheduled immutable batches

`piddi sync` runs once and exits. A timer can run it regularly. Each run publishes
ready batches, then maps projects that have enough pending changes or have
waited long enough. Defaults are 1,000 distinct changed items or one hour.

```bash
piddi sync
piddi sync --force-map
piddi sync --project cmip6 --workers 4 --retries 3
```

Sync uses `consumer.projects`. Repeated `--project` options restrict work to
selected configured projects. Unknown or unconfigured names fail before publication.
`--force-map` skips mapping thresholds for pending items. Ready batches publish
regardless of those thresholds. A shared lock prevents overlapping sync runs.

Each batch holds up to `batch.max_items` catalog items (default: 2,000). An item's
Handles stay together. Changes to one item count once and keep the oldest pending
time. Changes arriving during a sync run remain for the next run. Eligible
projects take turns; missing catalogs are skipped until harvest creates them.

Every included asset must validate. Mapping failures leave diagnostic files in
`.building-*` directories and keep the catalog items pending. Successful mapping
saves a fixed `handles.jsonl` and a manifest under
`batches/<project>/ready/<batch-id>/`. The manifest records the project, settings,
catalog generation, sequence bounds, counts, and checksum.

The mapping checkpoint advances only after the batch is saved durably. If Piddi
stops between those steps, the next run recovers from the manifest. Publication
checks the saved batch and does not append to it.

Failed publication leaves the batch ready and blocks newer work for that project.
Other projects can continue. A later run repeats the failed batch. Successful
batches move to `batches/<project>/published/YYYY/MM/DD/<batch-id>/`, partitioned
by the UTC creation date in the batch ID. The path stays stable across retries
and midnight recovery. Existing flat published archives remain in place and
are not republished. Only ready work is scanned by sync, so growing archive
history does not add to that scan. A crash can repeat successful
Handle writes because the server write and local completion marker are separate.

Publication uses four workers by default. Projects and batches run in sequence;
workers send different Handles concurrently within a batch. Use `--workers 1`
for serial publication. `--workers` and `--retries` override `[batch]` settings
for the current run, including existing ready batches.

Changing mapping settings alone does not mark old items pending. To rebuild their
Handle content, run manual mapping and publication. Manual runs do not advance
the sync checkpoint. Sync batches have no automatic retention policy.
See [Deployment](deployment.md#initial-deployment)
to enable the timer.

## Deferred publication

Publish completed Handle files after inspecting them:

```bash
piddi publish --project cmip6 --date yesterday
piddi publish --project cmip6 --from 2026-10-08 --to yesterday
piddi publish --project cmip6 --date yesterday --offset 1000 --limit 1000 --retries 3
piddi publish --project cmip6 outputs/cmip6/handles/handles_2026-10-08.jsonl
```

Explicit inputs can be files or directories. Do not combine them with date
selectors. Date ranges require `--project`, skip days without files, and publish
available files oldest first. An empty range fails before publication.

Offsets count nonblank lines across all selected files. Limits count attempted
records, including failures. The command keeps its input files and uses Handle
overwrite requests. Repeating a fixed input can repeat earlier writes. Any failed
record makes the command exit with a nonzero status; earlier writes stay published.

`--retries` retries temporary connection errors, timeouts, rate limits, and server
errors. Manual publish defaults to no retries. The delay starts at one second
and doubles up to 60 seconds. Set `--retry-delay` to change the initial delay.
Invalid credentials and other permanent client errors are not retried.

`--workers N` sends different Handles concurrently. Writes to the same Handle
stay in source order within the run. Input is read in bounded batches without
counting the whole file first, so the progress total can be unknown.

`--project NAME` checks every record before sending it. Invalid records get failure
receipts while valid records continue. Without `--project`, the first parsed
record selects the service; later records must match its project. Publish each
project's records separately. See [Handle profiles](configuration.md#multiple-handle-servers).

### Publication receipts

Each attempt writes a JSONL receipt under `published/YYYY/MM/DD/` and prints its
path. The date is the UTC start date of the receipt. Existing flat receipt files
remain in place; explicit output paths are used exactly as supplied.
Receipt names use UTC timestamps and get a suffix if the name already exists.
Each line records the outcome, PID, action, URL, project, source position, retry
count, and error. Dataset and asset metadata come from the current input record;
missing values remain `null`.

Concurrent completion can change receipt line order. Use `position`, `batch_index`,
`source_file`, and `source_line` to locate an input record. Positions cover the
whole selection, and `batch_total` is `null`. Receipts keep every failure; the
terminal summary shows at most 100 error messages.

## Runtime output

JSONL files store one JSON object per line. Paths below are relative to `consumer.output_dir`:

| Directory | Contents |
| --- | --- |
| `dump/` | Original Kafka messages from harvest, saved before project routing. |
| `catalog/` | One SQLite catalog per registered project encountered. |
| `<project>/handles/` | Daily prepared Handle JSONL, including the project name. |
| `batches/<project>/` | Sync batches, manifests, and failed mapping files. |
| `published/YYYY/MM/DD/` | Per-record publication receipts, grouped by UTC date. |
| `<project>/failures/r0/` | Failed STAC records and error details. |
| `<project>/skipped/` | Skipped diagnostic records from legacy processing. |
| `failures/`, `skipped/` | Diagnostics without a resolved project. |

Raw dumps preserve the order in which Piddi consumed events. They are useful
for investigation. Map reads catalogs, not dumps or failure files.
See [Recovery](recovery.md) before repeating failed work.

## Terminal progress

Harvest shows events, catalog writes, errors, elapsed time, and processing rate.
Map shows catalog items and prepared Handles. Sync shows the project and current
phase. Publication shows completed records and, when known, the batch total and
estimated time remaining.

Use `--silent` or `--no-progress` before the command to hide progress bars:

```bash
piddi --no-progress map --project cmip6
```

Final summaries remain visible. Counters describe the current run, not the total
catalog size. Ctrl-C closes the input, progress display, and monitoring reporter.

## Logging and statistics

INFO is the default log level. Local runs write to `pid.log`; deployed services
normally write to `/var/log/piddi/piddi.log`. Use `--log PATH` to change the file
for one command. Use `-vv` or `--debug` for individual record details.

Log summaries use labels such as `[harvest]`, `[map]`, `[publish]`, `[sync/map]`,
and `[sync/publish]`. Harvest and mapping summaries appear every 30 seconds or
10,000 messages as work arrives. Publication logs progress every 30 seconds as
results arrive, then logs a final summary. No publication progress message is
emitted while waiting for a result.

Publication summaries count successes, failures, and retries for that attempt.
They do not count unique Handles across runs. Receipts keep per-record details.
Successful individual writes appear at DEBUG; failures appear at ERROR.

The optional monitoring database tracks harvest, mapping and publication.
Read the database with:

```bash
piddi top
piddi top --once
piddi top --project cmip7 --once
piddi top --json
piddi top --details --history 360
```

`top` is read-only and needs no Kafka or Handle connection. It reports an error
if the database is missing or invalid. A heartbeat updates every five seconds,
even when Kafka is idle. An unfinished run is marked stale after 15 seconds
without a heartbeat. The default view shows disk space, harvest status and one
row per project: pending catalog items, recorded prepared/published Handle totals,
activity, publication speed and estimated batch completion. Unknown estimates
appear as a dash. Errors and stale heartbeats appear below the table. Stale run
records superseded by newer runs are hidden from the display; JSON retains them.
Use `--details` for batch health, last publication times, individual runs and history.

History samples are saved every 15 seconds and at startup and shutdown. The
history window covers one hour; `--details --history 360` shows six hours. Samples are kept
for 30 days. They contain counters, not raw messages or log text. Catalog writes
exclude duplicate events. Schema 3 adds catalog counters automatically on writer
startup; `top` can also read schema 2.

See [Logging and monitoring](configuration.md#logging-and-monitoring) to change
these defaults. File logging follows log rotation without a service restart.

## Real Handle service contract test

Live tests create Handles and leave them on the server. Use a disposable test
prefix and set credentials through the environment:

```bash
export PIDDI_LIVE_HANDLE_SERVER_URL=https://handle-test.example/api-root
export PIDDI_LIVE_HANDLE_PREFIX=21.TEST
export PIDDI_LIVE_HANDLE_USERNAME='300:21.TEST/testuser'
export PIDDI_LIVE_HANDLE_PASSWORD='...'
pytest -m live tests/live/test_real_handle_service.py
```

The tests check create, update, read-back, concurrent publication, and same-PID
ordering. Set `PIDDI_LIVE_HANDLE_VERIFY_HTTPS=false` only for a trusted test
service with a self-signed certificate.

The Docker mock delays valid PUT requests by 50 ms. To change the delay:

```bash
PIDDI_MOCK_HANDLE_PUT_DELAY_SECONDS=0.1 docker compose up -d
```

## Local cleanup

### Closed raw dumps

Run maintenance manually before enabling its daily schedule:

```bash
piddi maintain --dry-run
piddi maintain
```

Maintenance compresses `dump/dump_messages_YYYY-MM-DD.jsonl` files dated before
today in UTC, using gzip level 1. It skips today's file, future dates, symbolic
links, and files with an existing `.jsonl.gz` archive. A lock prevents overlapping
maintenance runs. The original remains until the gzip output is complete and
durably stored; a dump that changes during compression is preserved with an
error. Compression needs temporary free space and observes the disk guard.

Only closed raw dumps are compressed. Batch payloads and receipts remain intact
because manifests reference them. There is no age-based deletion or off-host
archiving. Inspect compressed dumps with `gzip -cd <file.jsonl.gz>`; mapping uses
the catalog and does not need the raw dump files.

### Disk and project health

`piddi top` shows disk headroom, pending catalog items and Handle totals.
`piddi top --details` and `piddi top --json` also show ready and failed batch
counts, and the latest successful sync publication.
Failed publication batches remain ready and also count as failed.
The project table keeps prepared and published Handle totals visible while
harvest is active and after mapping or publication finishes. These totals sum
recorded outcomes in the monitoring database, including alias Handles. Repeated
mapping or publication runs count again; these are not counts of unique PIDs.
Publication now records its own live run, counters, heartbeat, and history, with
the same monitoring intervals as harvest. Existing publication receipts are not
backfilled: a dash means no publication counters have been recorded yet, and
publication totals start with runs using this monitoring support. Monitoring
must be enabled with `stats.enable_db = true`.

The compact project table includes publication speed and approximate batch
completion in UTC. With `--details`, the publication estimates table also shows
remaining Handles and the duration separately.
Sync supplies the exact batch Handle count from its manifest; recent speed uses
up to sixty seconds of monitoring samples with a five-second warmup. Old runs
and manual publications with no known total show an unavailable batch ETA.
Errors, stale heartbeats, or no recent progress suppress the end-time estimate.

The separate **Known backlog ~** estimate combines remaining ready Handles with
pending catalog items multiplied by the Handles-per-item ratio from up to five
recent mapping runs for each project. It deducts confirmed progress in the active
batch, so that batch is not counted twice. This is a snapshot of known local
work, excluding unread Kafka messages, future arrivals, mapping time, scheduling
waits, and additional retries. The estimate updates as harvest discovers more
items. Missing counts or ratios hide this estimate in the compact view and show a
waiting state with `--details`. All estimate fields are also available in `top --json`.
The checks are read-only and report unavailable or older catalogs without
migrating them. Publication status uses a small `last-publication.json` marker
per project, written by successful sync runs; old archives are not scanned. A
project has no last-publication value until a new sync run records a success.

Monitor disk use and choose a retention policy for dumps, Handle files, batches,
and receipts. Piddi does not remove old runtime output automatically.

`make clean` removes build, bytecode, and test files. `make clean-dist` removes
other ignored development files too, with exceptions for runtime output, logs,
databases, local configuration, and selected environment and editor directories.
Check its exclusions in the Makefile before using it with other local folders.

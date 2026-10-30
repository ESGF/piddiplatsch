# Recovery

Fix the cause of a failure, then repeat the normal command. Failure and skipped
files are for inspection. Commands do not read them as inputs, and there is no
`retry` command.

## Find the failed record

Failures include the STAC record, error, and available project and source details:

```text
outputs/<project>/failures/r0/failed_items_<date>.jsonl
outputs/<project>/skipped/skipped_items_<date>.jsonl
```

Records without a resolved project use the global `failures/` or `skipped/`
directory. Original Kafka messages are in `outputs/dump/`.

## Restart harvest

Harvest stops on an unresolved event and leaves it unacknowledged. Repair the
configuration, source data, remote service, or storage problem, then run:

```bash
piddi harvest
```

Local checkpoints skip events already saved, so stored patches are not applied
twice. Harvest has no option to skip an unresolved event.

A patch without a local base triggers a remote STAC fetch. Recovered items stay
marked for refresh on later events and startup. If STAC is behind Kafka, wait
for it to catch up and repeat harvest before mapping. Remote recovery uses
current state and cannot restore missing historical versions.
See [Remote current-item recovery](operations.md#remote-current-item-recovery).

Do not delete catalogs as a restart step. If you need a rebuild, stop other users
of the catalog and follow [Rebuild catalogs](operations.md#rebuild-catalogs).
Rebuilding depends on retained Kafka history and available remote items.

## Repeat mapping or publication

Mapping stops at `consumer.max_errors`. Fix the cause, then select the same
catalog items again:

```bash
piddi map --project cmip6 --date yesterday
# Or repeat part of a date range
piddi map --project cmip6 --from 2026-10-08 --to yesterday --offset 1000 --limit 1000
```

Map appends to daily Handle files, so repeated mapping can add duplicate records.
Inspect the output before publishing. Mapping offsets count catalog items;
publication offsets count Handle records. These are different units.

```bash
piddi publish --project cmip6 --date yesterday --retries 3
```

Publication keeps its inputs and writes a receipt for every result. Repeating
an input overwrites the same Handles and can repeat earlier successful writes.
`--retries` handles temporary HTTP failures within the run.
See [Deferred publication](operations.md#deferred-publication) for ordering and
selection rules.

## Scheduled batch recovery

Failed sync mapping leaves files under `batches/<project>/.building-*` for
inspection. The source items stay pending. A batch becomes ready only after
mapping succeeds.

Failed publication keeps the fixed batch under `batches/<project>/ready/`.
Fix the cause, then repeat `piddi sync`. It retries that batch before newer work
for the project. Successful batches move to `batches/<project>/published/`.
A crash can repeat Handle writes already sent to the server.

A checksum or catalog generation mismatch blocks publication. Inspect the batch
before making changes. Stop sync before cleaning catalogs, and finish or
explicitly archive the old ready queue first.

Back up catalogs, `.harvest-state.json`, and the batch directory together.
Catalog progress can refer to saved batches that have not yet been published.

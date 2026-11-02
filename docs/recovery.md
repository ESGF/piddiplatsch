# Recovery

This page is for the service maintainer. For routine checks and reporting, use
[Admin](admin.md). Take a [backup](deployment-reference.md#backups) before changing
stored state.

## Interrupted processing

- **Harvest:** fix the reported cause and restart. Stored Kafka checkpoints skip
  events already committed locally. If required history has expired from Kafka,
  restore the missing interval from dumps before resuming.
- **Map:** repeat map or sync. Uncommitted snapshots stay pending; committed Handle
  payloads stay in the database.
- **Publish:** fix the connection or server error and repeat publish or sync.
  Committed writes are skipped. A write may repeat if the process stopped after
  remote success but before its local acknowledgement.

Older failed publication blocks newer batches for that project. Other projects
can continue. Keep legacy ready exports until imported into the SQLite outbox.

## Invalid records

Check `failures.jsonl` and the error log. A corrected Kafka event creates new work;
old invalid snapshots do not retry automatically. `piddi retry --help` explains
limited retries before successful project publication.

State-changing recovery commands live under `piddi admin`. They preview by
default; `--apply` executes the plan. Consult each command's help before use.
Resetting or rebuilding history is a maintainer task, not a routine monitoring step.

## Disk safety stop

Low space or a failed storage check creates `.disk-space-stop` under the output
directory. Services remain blocked even after space recovers or the host reboots.

Stop the services, inspect the marker and logs, and restore sufficient free space
(preferably above the warning threshold). Only after resolving the cause, remove
the marker and start the services. No database reset is needed.

## Restore from backups

Prefer a matching database and data-directory backup when publication progress
must be preserved. Stop all Piddi services before restoring it.

Original `dump_messages_*.jsonl` and `.jsonl.gz` files can rebuild harvest state
in a fresh output directory using `piddi admin recover --archive PATH`.
Preview first, then use `--apply` with a dedicated recovery configuration.
Include all stored projects and preserve original filenames and source coordinates.

After replay, normal harvest resumes from database checkpoints if Kafka retains
the following interval. A gap missing from both dumps and Kafka cannot be recovered.
Dump replay reconstructs pending work and can republish previously completed
Handles; it does not restore publication acknowledgements.

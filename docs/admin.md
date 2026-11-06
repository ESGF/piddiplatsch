# Admin

Check progress, read logs, and control the deployed services.
Commands below use the default installation paths.

## Progress

Open a root shell with `sudo -i`, or use an existing root login:

```console
sudo -i
piddi top
```

`piddi top` works from any directory and loads the installed configuration.
The full command path is `/usr/local/sbin/piddi`. Ctrl+C closes the display
without stopping the services. Use `piddi top --once` for a single snapshot.

| Status | Meaning |
| --- | --- |
| Harvest heartbeat updating | Harvest is running, including when Kafka is idle. |
| `idle` | No pending work. |
| `waiting` | Waiting for the batch count or time threshold. |
| `ready` | Work is eligible for the next batch. |
| `queued` | Mapped Handles are waiting to publish. |
| `preparing`, `mapping`, `sealing`, `verifying`, `publishing`, `finalizing` | A batch is being processed. |
| `sampling` | Counts are loading. |
| `disabled` | Automatic processing is disabled for the project. |
| `stale` | The heartbeat has stopped updating. Check the logs. |

Batches start at 100 pending events or after 10 minutes by default. Projects
may wait while another batch finishes. Pending work can grow during catch-up.

**Errors** includes earlier failures; increases indicate new errors. Check log
timestamps to match them to the current run. Rates and completion times are estimates.
Rejected harvest events count alongside mapping/publication errors. A failed
harvest shows `Harvest: failed` with its saved error summary until a newer
harvest run supersedes it; completed mapping or publication does not hide it.

Disk warnings start below **40 GiB** free; processing stops below **20 GiB** by
default. See [Recovery](recovery.md#disk-safety-stop) for resuming after a disk stop.

## Logs

Show the latest errors:

```console
tail -n 100 /var/log/piddi/piddi.error.log
```

Follow progress live (Ctrl+C to exit):

```console
tail -f /var/log/piddi/piddi.log
```

Logs show connection and authentication failures, rejected records, publication
errors, and disk warnings. For system messages from today:

```console
journalctl -u piddi.service -u piddi-sync.service --since today --no-pager
```

## Services

From the root shell, go to the install folder:

```console
cd /opt/piddiplatsch
```

| Command | Action |
| --- | --- |
| `make status` | Show service and timer states. |
| `make start` | Start services and timers. |
| `make stop` | Stop services and timers. |
| `make restart` | Stop and start services and timers. |

Harvest runs continuously; the sync timer schedules mapping and publication.
The sync service is normally inactive between runs.
Start and restart also trigger sync, which can publish Handles.

## Switch CMIP7 to the production PID service

With complete CMIP7 work history retained, run as root (default paths):

```bash
cd /opt/piddiplatsch
make stop
# Optional backup:
# tar -czf "/root/piddi-backup-$(date +%Y%m%dT%H%M%S).tar.gz" \
#     /etc/piddi /var/lib/piddi custom.toml

# Preview, then reset CMIP7.
piddi admin reset --project cmip7
piddi admin reset --project cmip7 --apply

# Set the production Handle server, credentials, and prefix.
vim custom.toml

piddi config explain --project cmip7
# Optional: map and inspect a sample.
# piddi map --project cmip7 --limit 1
# less /var/lib/piddi/batches/cmip7/ready/*/handles.jsonl

# Deploy the changes and restart.
make play
make restart
```

The final commands can publish immediately. For custom data paths, adjust the
backup and sample inspection paths; see [Backups](deployment-reference.md#backups).

Reset keeps the dumps and database, preserving STAC records and Kafka checkpoints;
no Kafka replay is needed.
It remaps retained work and does not delete Handles from the old service.
If needed, stop the services and repeat the reset.

# Deployment

Use this guide to run Piddi on a Linux host with systemd. A host administrator
with sudo privileges sets it up and runs updates. Ansible creates the `piddi`
user for the services.

There are two jobs: **harvest** keeps the local catalogs up to date, and
**sync** maps changes and publishes Handles.

## Initial deployment

### 1. Install and configure

Install Git, Make, and Conda on the host first. The commands below open a root
shell. Make sure Conda is on `PATH` in that shell:

```console
sudo -i
git clone https://github.com/ESGF/piddiplatsch.git /opt/piddiplatsch
cd /opt/piddiplatsch
python -m pip install ansible-core
cp etc/esgf-example.toml custom.toml
```

Edit `custom.toml`. Set your Kafka connection, credentials, projects, and Handle
profiles. See [Configuration](configuration.md) for the settings.

Install the application and services:

```console
make deploy
```

This also creates the data directories and sets up log rotation. Harvest and
sync stay off until you enable them.

### 2. Start harvest

Create the deployment settings file:

```console
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
```

Add this setting to `deploy/ansible/custom.yml`:

```yaml
piddi_enable_service: true
```

Apply it:

```console
make play
```

Harvest now runs continuously and starts at boot. It fills the local catalogs
without publishing Handles. Use the monitoring commands below to check it.

### 3. Enable publication

Check your Handle server, credentials, and prefixes before enabling sync.
For an optional mapping check, follow [Check before publication](deployment-reference.md#check-before-publication).

Add this setting to `deploy/ansible/custom.yml`, keeping harvest enabled:

```yaml
piddi_enable_service: true
piddi_enable_sync_timer: true
```

Then run:

```console
make play
```

**Sync now publishes real Handles.** The timer checks for work every minute.
By default, it maps a project after 10,000 changed items or one hour of waiting.
See [Scheduled batches](configuration.md#scheduled-batches) to change these limits.

## Run updates

Run these commands as root, with Conda on `PATH`:

```console
cd /opt/piddiplatsch
make stop
git pull --ff-only
make deploy
make restart
```

Keep your existing configuration files. If an update fails, fix the error before
restarting. `make deploy` updates packages and applies settings. It may start
enabled services. The final restart loads the new code and starts a sync run.

For settings changes only, edit the relevant file and run `make play`:

| File | What to change |
| --- | --- |
| `custom.toml` | Kafka, Handles, projects, and mapping settings. |
| `deploy/ansible/custom.yml` | Service and timer settings. |

Do not edit `/etc/piddi/piddi.toml` directly; Ansible replaces it.
Changing mapping settings does not remap old items automatically. See
[Operations](operations.md#scheduled-immutable-batches) if you need to rebuild Handles.

## Monitor

Use `piddi.log` and `piddi top` for daily monitoring.

Follow the application log:

```console
tail -f /var/log/piddi/piddi.log
```

The log shows harvest, mapping, and publication progress. Check for repeated
errors and failed publication.

In another terminal, view live progress:

```console
cd /var/lib/piddi
piddi top
```

Run this from the administrator shell used for deployment. Piddi loads the
installed site configuration. Starting from `/var/lib/piddi` avoids loading the
checkout's local settings.

`top` shows harvest and mapping counts, errors, and heartbeats. An idle topic is
healthy while the heartbeat keeps updating. Publication results are in
`piddi.log`.

If progress stops, see [Service troubleshooting](deployment-reference.md#service-troubleshooting).

## Stop and restart

As root, from `/opt/piddiplatsch`:

```console
make stop
make start
make restart
```

Choose the command you need. `start` and `restart` also start sync and request
immediate publication of ready work. These commands control the current runtime;
Ansible settings control startup at boot.

For backups, custom paths, older deployments, and Vagrant, see
[Deployment reference](deployment-reference.md). For failed work, see
[Recovery](recovery.md).

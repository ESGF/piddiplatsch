# Ansible deployment

A host administrator runs these steps on Linux with systemd. Install Git, Make,
Conda, and Python first. Keep Conda on `PATH` in the root shell.

## Install

```console
sudo -i
git clone https://github.com/ESGF/piddiplatsch.git /opt/piddiplatsch
cd /opt/piddiplatsch
python -m pip install ansible-core
cp etc/esgf-example.toml custom.toml
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
```

Edit `custom.toml` with your Kafka connection, projects, and Handle profiles.
Then install the environment, configuration, services, and log rotation:

```console
make deploy
```

Harvest and publication stay disabled until explicitly enabled.

## Enable harvest

Set this in `deploy/ansible/custom.yml`:

```yaml
piddi_enable_service: true
```

Apply it:

```console
make play
```

Harvest now runs continuously and starts at boot. Check it from the root shell;
no directory change is needed:

```console
piddi top
```

Follow the log in another terminal:

```console
tail -f /var/log/piddi/piddi.log
```

Daily maintenance is enabled with harvest by default. It compresses closed files;
deletion stays disabled unless configured.

## Enable publication

Check the Handle server, credentials, prefixes, and mapping output first. See
[Check before publication](deployment-reference.md#check-before-publication).
Keep harvest enabled and add:

```yaml
piddi_enable_service: true
piddi_enable_sync_timer: true
```

From `/opt/piddiplatsch`, run `make play`.
**Sync now publishes real Handles.** It maps at 100 pending events or 10 minutes,
in batches of up to 2,000 events by default. The timer checks again 20 seconds
after each run finishes.

## Updates and settings

As root, with Conda on `PATH`:

```console
cd /opt/piddiplatsch
make stop
git pull --ff-only
make deploy
make restart
make status
```

Keep your configuration files and take a [backup](deployment-reference.md#backups)
before upgrading. If deployment fails, resolve it before restarting.

For settings changes only, edit `custom.toml` (application settings) or
`deploy/ansible/custom.yml` (services and timers), then run `make play` from the
checkout. Ansible replaces `/etc/piddi/piddi.toml`; edit the source file instead.
Mapping changes affect future mapping, not already prepared Handles.

From the install folder (default `/opt/piddiplatsch`), use `make start`,
`make stop`, `make restart`, or `make status` to control and check deployed services.
Start and restart also request an immediate sync run, which can publish Handles.
Ansible settings control startup at boot.

Use [Admin](admin.md) for daily checks and reporting errors. Custom paths,
backups, and the test VM are in the [deployment reference](deployment-reference.md).

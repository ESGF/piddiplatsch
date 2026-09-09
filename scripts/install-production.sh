#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)

if [ "$(id -u)" -ne 0 ]; then
    echo "Run this installer as root (for example with sudo)." >&2
    exit 1
fi

piddi_executable=${PIDDI_EXECUTABLE:-}
if [ -z "$piddi_executable" ]; then
    piddi_executable=$(command -v piddi || true)
fi
if [ -z "$piddi_executable" ] || [ ! -x "$piddi_executable" ]; then
    echo "Could not find an executable piddi command." >&2
    echo "Set PIDDI_EXECUTABLE to its absolute path and retry." >&2
    exit 1
fi
case "$piddi_executable" in
    /*) ;;
    *)
        echo "PIDDI_EXECUTABLE must be an absolute path." >&2
        exit 1
        ;;
esac
case "$piddi_executable" in
    *[!A-Za-z0-9_./+-]*)
        echo "PIDDI_EXECUTABLE contains unsupported characters: $piddi_executable" >&2
        exit 1
        ;;
esac

for command_name in getent groupadd install logrotate runuser sed systemctl useradd; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Required command not found: $command_name" >&2
        exit 1
    fi
done

if ! getent group piddi >/dev/null 2>&1; then
    groupadd --system piddi
fi
if ! id piddi >/dev/null 2>&1; then
    useradd --system --gid piddi --home-dir /var/lib/piddi \
        --shell /usr/sbin/nologin piddi
fi

install -d -o root -g piddi -m 0750 /etc/piddi
install -d -o piddi -g piddi -m 0750 /var/lib/piddi /var/log/piddi

if [ ! -e /etc/piddi/piddi.toml ]; then
    install -o root -g piddi -m 0640 \
        "$project_dir/etc/piddi-production.toml" /etc/piddi/piddi.toml
    echo "Installed /etc/piddi/piddi.toml; edit its example endpoints before use."
else
    echo "Kept existing /etc/piddi/piddi.toml."
fi

unit_tmp=$(mktemp)
trap 'rm -f "$unit_tmp"' EXIT HUP INT TERM
sed "s|@PIDDI_EXECUTABLE@|$piddi_executable|g" \
    "$project_dir/etc/systemd/piddi.service.in" >"$unit_tmp"
install -o root -g root -m 0644 "$unit_tmp" /etc/systemd/system/piddi.service
install -o root -g root -m 0644 \
    "$project_dir/etc/systemd/piddi-logrotate.service" \
    /etc/systemd/system/piddi-logrotate.service
install -o root -g root -m 0644 \
    "$project_dir/etc/systemd/piddi-logrotate.timer" \
    /etc/systemd/system/piddi-logrotate.timer
install -o root -g root -m 0644 \
    "$project_dir/etc/logrotate/piddi" /etc/logrotate.d/piddi

systemctl daemon-reload

echo
echo "Production files installed. The piddi service has not been started."
echo "Next steps:"
echo "  1. Edit /etc/piddi/piddi.toml"
echo "  2. Validate: runuser -u piddi -- $piddi_executable --config /etc/piddi/piddi.toml config validate"
echo "  3. Test: runuser -u piddi -- $piddi_executable --config /etc/piddi/piddi.toml --silent consume"
echo "  4. Enable: systemctl enable --now piddi piddi-logrotate.timer"
echo "  5. Inspect: systemctl status piddi"
echo "              tail -f /var/log/piddi/piddi.log"

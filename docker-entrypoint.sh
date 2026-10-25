#!/bin/sh
# Start as root only long enough to map the app user to PUID/PGID and fix
# ownership of /data, then drop privileges.
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

if [ "$(id -u)" != "0" ] || [ "$PUID" = "0" ]; then
    exec "$@"
fi

groupmod -o -g "$PGID" pigeonhole
usermod -o -u "$PUID" -g "$PGID" pigeonhole
mkdir -p "$DATA_DIR"
if [ -n "$(find "$DATA_DIR" \( ! -uid "$PUID" -o ! -gid "$PGID" \) -print -quit)" ]; then
    chown -R "$PUID:$PGID" "$DATA_DIR" || echo "warning: could not chown $DATA_DIR" >&2
fi

export HOME=/home/pigeonhole USER=pigeonhole
exec setpriv --reuid="$PUID" --regid="$PGID" --init-groups "$@"

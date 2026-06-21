#!/bin/sh
set -e
chown -R replicore:replicore /data 2>/dev/null || true
exec su replicore -c 'exec node /app/bin/docker-entrypoint.mjs'

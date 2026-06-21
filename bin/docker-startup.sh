#!/bin/sh
set -e
chown -R replicore:replicore /data 2>/dev/null || true
exec gosu replicore node /app/bin/docker-entrypoint.mjs

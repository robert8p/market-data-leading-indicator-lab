#!/bin/sh
set -eu
umask 077
mountpoint -q /var/data/eq17 || { echo 'dedicated_evidence_mount_required' >&2; exit 70; }
exec 9>/var/data/eq17/.recorder.lock
flock -n 9 || { echo 'existing_recorder_holds_mount' >&2; exit 70; }
exec node /app/worker.cjs

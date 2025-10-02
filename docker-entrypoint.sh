#!/bin/sh
set -e

# start cron in the background (don't block)
service cron start || true

# run the module with the SAME interpreter used for installs
# "$1" comes from CMD (fabric_ceph)
exec python -m "$1"

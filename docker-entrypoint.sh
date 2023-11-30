#!/usr/bin/env sh
set -e

if [ "$OPENFGA_MIGRATE_ON_INIT" = "1" ] && [ -n "$OPENFGA_DATASTORE_URI" ] ; then
  /openfga migrate
fi

exec "${@}"
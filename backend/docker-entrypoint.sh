#!/bin/sh
# Apply migrations at startup, for platforms with no release hook.
#
# Opt-in, and deliberately off by default. Alembic takes no lock, so if every
# replica ran this on boot, N replicas starting together would each try to apply
# the same revision and race. Platforms that have a release step (Fly's
# release_command, Render's preDeploy) should use that and leave this unset.
# Single-container hosts such as a Hugging Face Space have no such step, so they
# set SYNCPAD_MIGRATE_ON_START=1 and accept that there is only one of them.
set -e

case "${SYNCPAD_MIGRATE_ON_START:-}" in
  1 | true | TRUE | yes | YES)
    echo "entrypoint: alembic upgrade head"
    alembic upgrade head
    echo "entrypoint: migrations applied"
    ;;
esac

exec "$@"

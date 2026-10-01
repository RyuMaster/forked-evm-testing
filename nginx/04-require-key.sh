#!/bin/sh
# Refuse to start without a real app key: an empty one would make the tunnel gate's map entry "1|"
# match a request that carries no key at all (native spec 2026-10-01-play-internal-test §3.2).
if [ "${#TAURION_APP_KEY}" -lt 32 ]; then
  echo "04-require-key: TAURION_APP_KEY missing or too short; refusing to start" >&2
  exit 1
fi

#!/usr/bin/env bash
# Snapshot the three first-boot seed files before tearing the stack down,
# so the next fresh deploy can resume from this state.
#
# Stops every stack service except MariaDB to quiesce all writers
# (gsp, xayax, datacentre-updater, datacentre-api, etc.), then copies
# storage.sqlite and dumps the two MariaDB schemas.
#
# Outputs (in $OUT_DIR):
#   storage.sqlite    — raw SQLite for STORAGE_SQLITE_PATH
#   archival.sql      — mariadb-dump for ARCHIVAL_SQL_PATH
#   userconfig.sql    — mariadb-dump for USERCONFIG_SQL_PATH
#
# Usage:
#   scripts/snapshot-stack.sh [output-dir]
#
# Run from the repo root (compose stop needs the compose file).
#
# After this completes: `docker compose down -v` to wipe volumes,
# then redeploy fresh with the .env paths printed at the end.

set -euo pipefail

OUT_DIR="${1:-./stack-snapshot/$(date +%Y%m%d-%H%M%S)}"
STACK="${STACK:-svstackll}"
MARIADB_PASS="${MARIADB_PASS:-gsparchival}"

GSP="${STACK}-gsp-1"
MARIA="${STACK}-mariadb-1"

if ! docker inspect "$GSP" >/dev/null 2>&1; then
  echo "ERROR: container $GSP not found (set STACK=<prefix>)" >&2
  exit 1
fi
if ! docker inspect "$MARIA" >/dev/null 2>&1; then
  echo "ERROR: container $MARIA not found" >&2
  exit 1
fi
if [ ! -f docker-compose.yml ]; then
  echo "ERROR: run this from the repo root (docker-compose.yml not found)" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
OUT_ABS=$(cd "$OUT_DIR" && pwd)
echo "→ snapshot dir: $OUT_ABS"

# --- stop every service except MariaDB ---
mapfile -t SERVICES < <(docker compose config --services | grep -v '^mariadb$')
echo "→ stopping ${#SERVICES[@]} services (keeping MariaDB up for the dump)..."
docker compose stop "${SERVICES[@]}"

# --- 1. SQLite (gsp is now stopped, file is at rest) ---
echo "→ copying storage.sqlite..."
docker cp "$GSP:/xayagame/sv/polygon/storage.sqlite" "$OUT_DIR/storage.sqlite"

# --- 2. archival (the big one, may take minutes) ---
echo "→ dumping archival..."
docker exec "$MARIA" \
  mariadb-dump -uroot -p"$MARIADB_PASS" --single-transaction --quick \
  --skip-lock-tables archival > "$OUT_DIR/archival.sql"

# --- 3. userconfig ---
echo "→ dumping userconfig..."
docker exec "$MARIA" \
  mariadb-dump -uroot -p"$MARIADB_PASS" --single-transaction --quick \
  --skip-lock-tables userconfig > "$OUT_DIR/userconfig.sql"

# --- summary ---
echo
echo "DONE. Files:"
ls -lh "$OUT_DIR"
cat <<EOF

Snapshot complete. The stack is halted (MariaDB still running).

Next steps when you're ready to wipe and redeploy:

  docker compose down -v          # destructive — wipes all volumes

Then set in .env before bringing the new stack up:

  STORAGE_SQLITE_PATH=$OUT_ABS/storage.sqlite
  ARCHIVAL_SQL_PATH=$OUT_ABS/archival.sql
  USERCONFIG_SQL_PATH=$OUT_ABS/userconfig.sql

  docker compose up -d
EOF

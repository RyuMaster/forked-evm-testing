#!/usr/bin/env bash
# Snapshot the three first-boot seed files from a running stack so that
# a subsequent fresh deploy can resume from this exact state.
#
# Outputs (in $OUT_DIR):
#   storage.sqlite    — raw SQLite for STORAGE_SQLITE_PATH
#   archival.sql      — mariadb-dump for ARCHIVAL_SQL_PATH
#   userconfig.sql    — mariadb-dump for USERCONFIG_SQL_PATH
#
# Usage:
#   scripts/snapshot-stack.sh [output-dir]
#
# Env knobs:
#   STACK         Compose project prefix (default: svstackll)
#   MARIADB_PASS  Root password (default: gsparchival)
#   PAUSE_GSP     yes|no — briefly stop GSP for an atomic SQLite copy
#                 (default: yes — strongly recommended; SQLite WAL means
#                 a live `docker cp` can capture a torn database)
#   COMPRESS_ARCHIVAL  yes|no — gzip archival.sql.gz instead of plain
#                              .sql (default: no; MariaDB init handles both)

set -euo pipefail

OUT_DIR="${1:-./stack-snapshot/$(date +%Y%m%d-%H%M%S)}"
STACK="${STACK:-svstackll}"
MARIADB_PASS="${MARIADB_PASS:-gsparchival}"
PAUSE_GSP="${PAUSE_GSP:-yes}"
COMPRESS_ARCHIVAL="${COMPRESS_ARCHIVAL:-no}"

GSP="${STACK}-gsp-1"
MARIA="${STACK}-mariadb-1"

# --- pre-flight: containers exist and running ---
for c in "$GSP" "$MARIA"; do
  if ! docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true; then
    echo "ERROR: container $c is not running" >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR"
OUT_ABS=$(cd "$OUT_DIR" && pwd)
echo "→ snapshot dir: $OUT_ABS"

# --- 1. SQLite ---
SQLITE_IN_CTR="/xayagame/sv/polygon/storage.sqlite"
if [ "$PAUSE_GSP" = "yes" ]; then
  echo "→ stopping $GSP for atomic SQLite copy..."
  docker stop "$GSP" >/dev/null
  trap 'docker start "$GSP" >/dev/null 2>&1 || true' EXIT
  docker cp "$GSP:$SQLITE_IN_CTR" "$OUT_DIR/storage.sqlite"
  docker start "$GSP" >/dev/null
  trap - EXIT
  echo "→ $GSP restarted"
else
  echo "→ WARNING: copying SQLite while live; risk of torn database" >&2
  docker cp "$GSP:$SQLITE_IN_CTR" "$OUT_DIR/storage.sqlite"
fi

# --- 2. archival ---
echo "→ dumping archival (this is the slow one — minutes)..."
if [ "$COMPRESS_ARCHIVAL" = "yes" ]; then
  ARCHIVAL_OUT="$OUT_DIR/archival.sql.gz"
  docker exec "$MARIA" \
    mariadb-dump -uroot -p"$MARIADB_PASS" --single-transaction --quick \
    --skip-lock-tables archival | gzip > "$ARCHIVAL_OUT"
else
  ARCHIVAL_OUT="$OUT_DIR/archival.sql"
  docker exec "$MARIA" \
    mariadb-dump -uroot -p"$MARIADB_PASS" --single-transaction --quick \
    --skip-lock-tables archival > "$ARCHIVAL_OUT"
fi

# --- 3. userconfig ---
echo "→ dumping userconfig..."
docker exec "$MARIA" \
  mariadb-dump -uroot -p"$MARIADB_PASS" --single-transaction --quick \
  --skip-lock-tables userconfig > "$OUT_DIR/userconfig.sql"

# --- summary ---
echo
echo "DONE. Files:"
ls -lh "$OUT_DIR"
echo
cat <<EOF
To resume from this snapshot on the next fresh deploy, set in .env:

  STORAGE_SQLITE_PATH=$OUT_ABS/storage.sqlite
  ARCHIVAL_SQL_PATH=$OUT_ABS/$(basename "$ARCHIVAL_OUT")
  USERCONFIG_SQL_PATH=$OUT_ABS/userconfig.sql

Then:
  docker compose down -v          # wipes existing volumes — destructive
  docker compose up -d             # cold-boot, importing from the snapshot
EOF

#!/bin/sh

# The forked block height. When a persisted state snapshot already exists we
# still pass --fork-url / --fork-block-number (anvil requires both alongside a
# loaded state), but the snapshot's block environment takes precedence, so the
# chain resumes where it left off rather than re-forking.
if [ "${FORK_BLOCK_NUMBER}" = "latest" ]
then
  HEIGHT_ARG=""
else
  HEIGHT_ARG="--fork-block-number ${FORK_BLOCK_NUMBER}"
fi

# Persistent state snapshot on a mounted volume (see docker-compose.yml).
#
# WHY THIS EXISTS: anvil holds the forked chain purely in memory. If the
# container ever exits (a crash, an OOM, an upstream RPC hiccup that panics the
# node, a redeploy) and comes back WITHOUT this snapshot, it re-forks from
# scratch at FORK_BLOCK_NUMBER and mines a brand-new set of blocks with new
# hashes. xayax + the GSP, already synced past the fork point, then see the
# chain rewind thousands of blocks to different hashes -> "reorg beyond pruning
# depth" -> xayax aborts -> the GSP aborts -> the whole stack is wedged until a
# full manual reset. That is exactly the failure this file is guarding against.
#
# --state is anvil's alias for --load-state + --dump-state: it loads the
# snapshot on boot when the file exists and writes it on graceful exit.
# --state-interval also flushes it every N seconds, so even a hard SIGABRT/OOM
# loses at most a few seconds (a 1-2 block rewind, which is a trivial in-window
# reorg that xayax handles cleanly - NOT a beyond-pruning-depth abort).
# --preserve-historical-states keeps per-block state so post-reload lookups at
# historical block hashes still resolve. On this low-activity test chain the
# snapshots are tiny; if the volume ever grows unreasonably, dropping that one
# flag is the release valve (base --state still preserves the block/hash chain
# that xayax needs).
#
# NOTE: the snapshot pins the forked block. To change FORK_BLOCK_NUMBER (or to
# start over from a clean fork) you must wipe the volume, e.g.
#   docker compose down && docker volume rm <stack>_basechain_state
STATE_DIR="${STATE_DIR:-/state}"
STATE_FILE="${STATE_DIR}/anvil-state.json"
mkdir -p "${STATE_DIR}"

# exec so anvil becomes PID 1 and receives SIGTERM directly on `docker stop`,
# giving --state a chance to flush a clean snapshot on graceful shutdown.
#
# Fork-RPC resilience: a single timed-out storage fetch used to panic the node
# outright. These raise anvil's tolerance for a flaky/rate-limited upstream so a
# transient error is retried with backoff instead of taking the whole node down.
# (--retries default 5, --timeout default 45000ms.)
exec anvil \
  --fork-url "${ENDPOINT}" \
  ${HEIGHT_ARG} \
  --host "0.0.0.0" \
  --auto-impersonate \
  --block-time 5 \
  --state "${STATE_FILE}" \
  --state-interval 10 \
  --preserve-historical-states \
  --retries 15 \
  --timeout 120000 \
  --fork-retry-backoff 2000

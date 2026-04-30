# Datacentre Stack — Local Build & Full Indexing Design

**Date:** 2026-04-30
**Status:** Draft for implementation

## Goal

Extend `forked-evm-testing/docker-compose.yml` so that **every endpoint** of `datacentre_api` can be exercised against a fully self-contained local stack. Today the compose file references `${DATACENTRE_API_IMAGE}` and `${DATACENTRE_UPDATER_IMAGE}` and runs without Redis or any Graph indexing infrastructure, so subgraph-backed endpoints (`/leaderboards`, `/user_activity`, `/shop`, parts of `/market`) cannot return real data.

The change vendors both project sources, switches to local Docker builds, adds Redis, and adds a complete Graph Protocol indexing stack (`graph-node` + `ipfs` + `postgres`) with three vendored subgraphs auto-deployed at boot.

## Non-goals

- Reintroducing the legacy local Anvil fork or the legacy Python helper RPC server (the `basechain/` and `helper/` directories remain dormant).
- Hosting graph-node externally or making any of the new services reachable from the host (graph-node, ipfs, redis, graph-postgres are internal-only).
- Replacing the existing GSP SQLite migration mechanism in `gsp-init`.
- Indexing performance tuning. Cold-sync time is whatever the operator's `BLOCKCHAIN_RPC_URL` and the subgraph `startBlock` allow; this spec accepts that as-is.

## Inputs the operator must provide (unchanged)

- `BLOCKCHAIN_RPC_URL` — Polygon archival endpoint
- `BLOCKCHAIN_AUTH_HEADER`
- `ACCOUNTS_CONTRACT`
- `GSP_IMAGE`

`DATACENTRE_UPDATER_IMAGE` and `DATACENTRE_API_IMAGE` are removed from `.env.example` — the stack now builds these locally.

## Source layout (new)

```
forked-evm-testing/
├── datacentre_api/              copy of C:\WorkFiles\datacentre_api
├── datacentre_updater/          copy of C:\WorkFiles\datacentre_updater
├── subgraphs/
│   ├── stats/                   copy of C:\WorkFiles\stats-subgraph
│   ├── sv/                      copy of C:\WorkFiles\subgraph
│   └── democrit/                copy of C:\WorkFiles\democrit-evm\subgraph
├── subgraph-deploy/             new (Dockerfile + deploy.sh)
└── docker-compose.yml           extended
```

Each vendored copy is a complete, self-contained snapshot. Re-syncing from upstream is a manual `cp -r` (intentionally — Portainer-friendly, no submodule complexity).

## Service topology

### Existing services — unchanged
`nginx`, `xayax`, `gsp`, `gsp-init`, `healthcheck_xayax`.

### Existing services — modified

**`mariadb`**
- `db-init/init-databases.sh` adds `CREATE DATABASE IF NOT EXISTS bigquery_local;` alongside the existing `datacentre`/`userconfig`/`lapsed_users` creations.
- No new container.

**`datacentre-updater`**
- `image: ${DATACENTRE_UPDATER_IMAGE}` → `build: { context: ./datacentre_updater, dockerfile: docker/Dockerfile }` (the project's Dockerfile lives in a subfolder, matching its existing `deploy.sh -f docker/Dockerfile .` pattern).
- The runtime `sed` hack on `db_manager.py` (collation `utf8mb4_0900_bin` → `utf8mb4_bin`) is moved into `datacentre_updater/docker/Dockerfile` as a build-time `sed -i` step. A comment on that line explains why and points at this spec.
- Compose `entrypoint` simplified to just `["python", "main.py"]` (the previous shell wrapper is no longer needed).
- New env vars (compose-level defaults — operator never edits):
  - `BIGQUERY_DB_HOST=mariadb`, `BIGQUERY_DB_USER=root`, `BIGQUERY_DB_PASSWORD=gsparchival`, `BIGQUERY_DB_NAME=bigquery_local`
  - `SVC_POLYGON_SUBGRAPH_URL=http://graph-node:8000/subgraphs/name/democrit`
  - `POLYGON_STATS_SUBGRAPH_URL=http://graph-node:8000/subgraphs/name/stats`
  - `SV_SUBGRAPH_URL=http://graph-node:8000/subgraphs/name/sv`
  - `DATAPACK_URL=https://downloads.soccerverse.com/svpack/default.json`
  - `DUMP_OUTPUT_FOLDER=/dumps`
- Mounts: existing `gspdata:/xayagame:ro` plus new `datadumps:/dumps` (read-write).
- `depends_on`: existing `mariadb` + `gsp-init`, plus new `subgraph-deploy: service_completed_successfully`.

**`datacentre-api`**
- `image: ${DATACENTRE_API_IMAGE}` → `build: { context: ./datacentre_api, dockerfile: docker/Dockerfile }`.
- New `entrypoint`: `["sh", "-c", "set -a; . /shared/graph_hashes.env; set +a; exec python3 main.py"]`. The `set -a` / `set +a` brackets cause every variable assignment in the sourced file to be auto-exported into the process environment (a bare `.` would leave them as shell-local only and `os.getenv` in Python would not see them).
- New env vars (compose-level defaults):
  - `REDIS_HOST=redis`, `REDIS_PORT=6379`, `REDIS_DB=0`, `REDIS_PASSWORD=`
  - `GRAPH_POSTGRES_HOST=graph-postgres`, `GRAPH_POSTGRES_PORT=5432`, `GRAPH_POSTGRES_USER=graph-node`, `GRAPH_POSTGRES_PASSWORD=graph-node`, `GRAPH_POSTGRES_DB=graph-node`
  - `GRAPH_SUBGRAPH_SV_URL=http://graph-node:8000/subgraphs/name/sv`
  - `DEFAULT_PROFILE_PIC_URL=https://downloads.soccerverse.com/default_profile.jpg`
  - `DUMP_OUTPUT_FOLDER=/dumps`
  - `GAME_ID=${GAME_ID:-sv}`
  - `GRAPH_SUBGRAPH_STATS` and `GRAPH_SUBGRAPH_SV` are NOT set in compose; they are sourced at entrypoint time from `/shared/graph_hashes.env` (see below).
- Mounts: existing volumes plus `datadumps:/dumps:ro` and `graph_hashes_shared:/shared:ro`.
- `depends_on`: `mariadb` healthy + `redis` healthy + `graph-postgres` healthy + `subgraph-deploy: service_completed_successfully` + `gsp-init: service_completed_successfully`.

### New services

**`redis`** — `redis:7-alpine`. Volume `redis_data:/data`. Healthcheck `redis-cli ping`. No host port.

**`graph-postgres`** — `postgres:14`. Env `POSTGRES_USER=graph-node`, `POSTGRES_PASSWORD=graph-node`, `POSTGRES_DB=graph-node`, `POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8`. Volume `graph_postgres_data:/var/lib/postgresql/data`. Healthcheck `pg_isready -U graph-node`. No host port.

**`ipfs`** — `ipfs/kubo:latest`. Volume `ipfs_data:/data/ipfs`. Healthcheck via `ipfs id`. No host port.

**`graph-node`** — `graphprotocol/graph-node:latest`. Env:
- `postgres_host=graph-postgres`, `postgres_user=graph-node`, `postgres_pass=graph-node`, `postgres_db=graph-node`
- `ipfs=ipfs:5001`
- `ethereum=matic:http://nginx/chain` (the `BLOCKCHAIN_AUTH_HEADER` injection happens inside nginx, so graph-node sees authenticated traffic transparently)
- `RUST_LOG=info`
Depends on `graph-postgres` healthy + `ipfs` healthy. Healthcheck `curl -f http://localhost:8020` (admin endpoint). No host port. (Optionally expose 8000/8020 to host via a debug profile — out of scope for v1.)

**`subgraph-deploy`** — one-shot built from `./subgraph-deploy/`:
```dockerfile
FROM node:20-alpine
RUN apk add --no-cache postgresql-client
WORKDIR /work
RUN npm install -g @graphprotocol/graph-cli@^0.98
COPY deploy.sh /usr/local/bin/deploy.sh
RUN chmod +x /usr/local/bin/deploy.sh
ENTRYPOINT ["/usr/local/bin/deploy.sh"]
```
Mounts the three vendored subgraph dirs read-only at `/subgraphs/{stats,sv,democrit}` and `graph_hashes_shared` at `/shared` (read-write). Depends on `graph-node` healthy.

### New volumes
`redis_data`, `graph_postgres_data`, `ipfs_data`, `datadumps`, `graph_hashes_shared`.

## Subgraph deployment + hash capture

`subgraph-deploy/deploy.sh`:

```sh
#!/bin/sh
set -eu

mkdir -p /shared
: > /shared/graph_hashes.env

PG="psql -h graph-postgres -U graph-node -d graph-node -tAq"
export PGPASSWORD=graph-node

deploy_one() {
  name=$1; src=$2; envvar=$3

  # Copy to writable scratch (vendor mount is ro)
  rm -rf /work/$name
  cp -r "$src" /work/$name
  cd /work/$name

  npm install --omit=dev --no-audit --no-fund
  npx graph codegen
  npx graph build

  # graph create is idempotent if the subgraph name already exists
  npx graph create --node http://graph-node:8020 "$name" || true

  npx graph deploy \
    --node http://graph-node:8020 \
    --ipfs http://ipfs:5001 \
    --version-label v1 \
    "$name"

  # Robust hash capture: query graph-node's bookkeeping table.
  # `deployment_schemas.subgraph` is the IPFS deployment hash.
  hash=$($PG -c "SELECT subgraph FROM public.deployment_schemas WHERE name = '$name' ORDER BY id DESC LIMIT 1;")
  if [ -z "$hash" ]; then
    echo "ERROR: could not resolve IPFS hash for $name" >&2
    exit 1
  fi
  echo "${envvar}=${hash}" >> /shared/graph_hashes.env
  echo "Recorded ${envvar}=${hash}"
}

deploy_one stats    /subgraphs/stats    GRAPH_SUBGRAPH_STATS
deploy_one sv       /subgraphs/sv       GRAPH_SUBGRAPH_SV
deploy_one democrit /subgraphs/democrit GRAPH_SUBGRAPH_DEMOCRIT

echo "All subgraphs deployed. graph_hashes.env contents:"
cat /shared/graph_hashes.env
```

Why this hash-capture path: `lookup_subgraph_schemas()` in `datacentre_api/modules/base.py` queries `public.deployment_schemas` by IPFS hash to find the schema name. We use the same table in reverse (name → hash). It is graph-node's own ground truth and is stable across graph-cli versions.

The deploy script is idempotent on re-run: `npm install` is cached in the container layer (vendored sources + `npm install` baked into the image would be even better; deferred to a future improvement to keep this spec narrow). `graph create` is best-effort. `graph deploy` against unchanged source produces the same IPFS hash.

## Boot sequence and degraded states

| T (cold boot) | What's happening | What works |
|---|---|---|
| 0 | nginx, xayax, ipfs, graph-postgres, redis, mariadb (importing) start | nothing exposed yet |
| ~30s | graph-node ready (ipfs + graph-postgres healthy) | graph-node admin reachable internally |
| ~30s–30m | mariadb still importing `archival.sql` (first boot only) | nothing user-facing |
| ~1–2m after graph-node ready | `subgraph-deploy` finishes; `/shared/graph_hashes.env` written; subgraphs queued for indexing | graph-node starts indexing |
| after mariadb healthy AND subgraph-deploy completed | datacentre-api starts; datacentre-updater starts | All MariaDB-backed and SQLite-backed endpoints work fully |
| indexing in progress (hours-days) | graph-node fetches Polygon history via `/chain` | Subgraph-backed endpoints (`/leaderboards`, `/user_activity`, `/shop`, parts of `/market`) return progressively-more-complete data |
| indexing caught up | — | Every endpoint returns full data |

The operator monitors progress via `docker compose logs -f graph-node`.

## Configuration surface

### `.env.example` — operator-facing
Removes: `DATACENTRE_UPDATER_IMAGE`, `DATACENTRE_API_IMAGE`.
Keeps: `BLOCKCHAIN_RPC_URL`, `BLOCKCHAIN_AUTH_HEADER`, `ACCOUNTS_CONTRACT`, `GSP_IMAGE`, `STORAGE_SQLITE_PATH`, `ARCHIVAL_SQL_PATH`, `USERCONFIG_SQL_PATH`, `MARIADB_PORT`.
Adds: `GAME_ID` (default `sv`).

### Hardcoded in compose (operator never edits)
All Redis / graph-postgres / bigquery_local / DUMP_OUTPUT / subgraph URL settings. Rationale: these are stack-internal hostnames; making them configurable invites mistakes and produces no real flexibility (the only way to opt out of the local Graph stack would be to delete services, which is an architectural change rather than a config change).

## Patches to vendored sources

The vendored copies of `datacentre_api` and `datacentre_updater` are otherwise byte-identical to their `C:\WorkFiles\` counterparts. Two changes:

1. `datacentre_updater/docker/Dockerfile` gains:
   ```dockerfile
   # Patch upstream collation that MariaDB rejects.
   # See docs/superpowers/specs/2026-04-30-datacentre-stack-design.md
   RUN sed -i 's/utf8mb4_0900_bin/utf8mb4_bin/g' db_manager.py
   ```
   placed after the `COPY . .` line.

2. (No source patch needed for `datacentre_api` — entrypoint changes live in compose.)

The vendored subgraphs are byte-identical copies. They are mounted read-only into `subgraph-deploy`; the deploy script copies each into a writable scratch dir before running `npm install`/`graph build` so the vendored trees never gain `node_modules/`/`build/` artefacts.

## Verification checklist

After `docker compose up -d --build`:

1. `docker compose ps` — all services either `running (healthy)` or, for one-shots, `exited (0)`.
2. `docker compose logs subgraph-deploy` — three `Recorded GRAPH_SUBGRAPH_*=Qm...` lines.
3. `docker volume inspect forked-evm-testing_graph_hashes_shared` then read the file — three env-style assignments.
4. `curl localhost:8100/api/clubs?per_page=5` — returns JSON immediately (MariaDB-backed).
5. `curl localhost:8100/api/share_balances?per_page=5` — returns JSON immediately (MariaDB-backed).
6. `curl localhost:8100/api/leaderboards/...` — returns 200; data may be empty until graph-node has indexed enough blocks. Watch with `docker compose logs -f graph-node | grep block_number`.
7. `curl localhost:8100/api/user_activity/...` — same.
8. After indexing catches up: every endpoint in `openapi/` returns non-empty data for representative inputs.

## Risks and open questions

- **Indexing-time RPC cost**: the `BLOCKCHAIN_RPC_URL` will see millions of `eth_getLogs`/`eth_call` requests during cold sync. Operator must confirm their plan tolerates that. Out of scope to mitigate here.
- **graph-cli version drift**: pinned to `^0.98` in the deploy image. If a vendored subgraph's `package.json` requires a newer graph-cli, the build fails loudly — easier to update than to debug a silent deploy mismatch.
- **No retry on subgraph-deploy failure**: a transient graph-node startup race could fail deploy. The operator re-runs `docker compose up -d` to retry; the script is idempotent. Could add explicit retry logic if this proves flaky in practice.
- **`democrit` subgraph path**: in upstream the subgraph is a subfolder of the `democrit-evm` repo. Vendored at `./subgraphs/democrit/`, not `./subgraphs/democrit-evm/subgraph/`, to keep the three-sibling layout consistent.
- **Removing the runtime `sed` hack** on `db_manager.py`: build-time patch in the vendored Dockerfile. If upstream `datacentre_updater` ever re-syncs and Soccerverse meanwhile fixed the collation in source, the patch becomes a no-op (sed finds nothing) — safe.
- **Stack memory footprint**: graph-node + postgres + ipfs adds ~2–4 GB RAM. Document in CLAUDE.md.

## Out of scope (future work)

- Bake `npm install` into the `subgraph-deploy` image at build time (faster reboots).
- Expose graph-node admin/query endpoints to the host on a debug profile.
- Health gate that blocks `datacentre-api` startup until graph-node has progressed past some block threshold (today the API starts as soon as schemas are resolvable — endpoints just return less data while indexing).
- Add a `make resync-vendor` helper that re-copies the four vendored trees from `C:\WorkFiles\…`.
- Re-introduce the legacy local Anvil fork or the helper RPC server as opt-in profiles.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Docker Compose stack that runs a Soccerverse Game State Provider (GSP) plus its supporting services (Xaya X, MariaDB archival store, Redis cache, Graph Protocol indexing pipeline, datacentre updater + API) against a remote EVM RPC endpoint, all behind an Nginx reverse proxy on `localhost:8100`. Designed for Portainer-from-git deploys with operator-supplied pre-built images for the heavyweight services.

The legacy local Anvil fork and the Python helper RPC server are no longer wired into compose; the `basechain/` and `helper/` directories remain in the tree but are inert. `/chain` is now a pass-through to a real RPC endpoint.

## Common Commands

```bash
# Start the full environment (requires .env populated — see Configuration)
docker compose up -d

# Tear down (keeps volumes)
docker compose down

# Tear down + wipe all volumes (destructive — wipes mariadb_data, graph_postgres_data, etc.)
docker compose down --volumes

# Rebuild one of the locally-built helper services (nginx, mariadb, healthcheck_xayax)
docker compose build nginx
```

## Architecture

### Services (docker-compose.yml)

- **`nginx`** — reverse proxy, the only host-published service (port 8100). Renders `proxy.conf.template` via `envsubst`. Routes:
  - `/chain` → `${BLOCKCHAIN_RPC_URL}` (auth header injected, WebSocket-capable)
  - `/gsp` → `gsp:8600`
  - `/api` → `datacentre-api:8000`
- **`xayax`** — `xaya/xayax` (eth mode), connects via `http://nginx/chain`.
- **`gsp-init`** — one-shot Alpine. Seeds `${STORAGE_SQLITE_PATH}` into the `gspdata` volume and applies idempotent Soccerverse SQLite migrations.
- **`mariadb`** — built from `db-init/`. Creates `archival`, `datacentre`, `userconfig`, `lapsed_users`, `bigquery_local` databases on first start; imports `archival.sql` and `userconfig.sql`. Healthcheck `start_period` is 30 minutes for the slow archival import.
- **`gsp`** — `${GSP_IMAGE}` (Soccerverse GSP). Uses MariaDB `archival`, talks to `xayax`.
- **`redis`** — `redis:7-alpine`, used as cache by `datacentre-api`.
- **`graph-postgres`** — `postgres:14`, locale C, UTF8. Storage backend for `graph-node`.
- **`ipfs`** — `ipfs/kubo`, manifest store for `graph-node`.
- **`graph-node`** — `graphprotocol/graph-node`. Indexes `matic` via `http://nginx/chain`. Internal-only.
- **`subgraph-deploy`** — `${SUBGRAPH_DEPLOY_IMAGE}`, one-shot. For each of `./subgraphs/{stats,sv,democrit}`: skips if any ABI contains the `_DATACENTRE_STACK_PLACEHOLDER` marker (see `subgraphs/democrit/POPULATE_ABIS.md`); otherwise copies the subgraph to writable scratch, runs `npm install` + `graph codegen` + `graph build` + `graph create` + `graph deploy`, then queries `graph-postgres` for the resulting IPFS hash and writes it to `/shared/graph_hashes.env` in the `graph_hashes_shared` volume.
- **`datacentre-updater`** — `${DATACENTRE_UPDATER_IMAGE}`. Reads SQLite + MariaDB; writes MariaDB (`datacentre`, `bigquery_local`) and the shared `datadumps` volume. Compose entrypoint applies the `utf8mb4_0900_bin` → `utf8mb4_bin` collation patch at runtime via `sed -i db_manager.py` (also applied at build time inside the vendored `Dockerfile`, so the patch is no-op-safe if your image already includes it).
- **`datacentre-api`** — `${DATACENTRE_API_IMAGE}`. FastAPI behind Gunicorn. Entrypoint sources `/shared/graph_hashes.env` (auto-exported via `set -a`) before starting Python so `lookup_subgraph_schemas()` resolves locally-deployed subgraph hashes against `graph-postgres`.
- **`healthcheck_xayax`** — sidecar, checks `xayax:8000` reachable.
- **`prometheus`** — `prom/prometheus:v2.55.1`, internal-only TSDB. Scrapes `datacentre-api:9100/metrics` every 15 s. 270-day retention. Volume `prometheus_data`.
- **`grafana`** — `grafana/grafana:11.3.1`, host-published on `${GRAFANA_PORT:-3000}`. Datasources (Prometheus + `MariaDB-datacentre`) and the "API Performance" dashboard auto-provisioned from `./monitoring/grafana/`. Default credentials `admin / ${GRAFANA_ADMIN_PASSWORD:-admin}`. Volume `grafana_data`.

### Service dependency chain

```
ipfs, graph-postgres, redis, mariadb, nginx, xayax — all start independently
graph-node             ← depends on ipfs, graph-postgres healthy
subgraph-deploy        ← depends on graph-node healthy (one-shot)
healthcheck_xayax      ← checks xayax
gsp-init               ← one-shot
gsp                    ← gsp-init complete + healthcheck_xayax healthy + mariadb healthy
datacentre-updater     ← mariadb + gsp-init + subgraph-deploy
datacentre-api         ← mariadb + redis + graph-postgres + gsp-init + subgraph-deploy
```

### Sources

```
forked-evm-testing/
├── subgraphs/             bind-mounted into subgraph-deploy at runtime
│   ├── stats/             xaya-stats subgraph
│   ├── sv/                sv-subgraph
│   └── democrit/          democrit-sv subgraph (placeholder ABIs — see POPULATE_ABIS.md)
└── subgraph-deploy/       Dockerfile + deploy.sh for the one-shot deployer (built into ${SUBGRAPH_DEPLOY_IMAGE})
```

`datacentre-api` and `datacentre-updater` are NOT vendored in this repo. Their sources live in separate upstream repos at `C:\WorkFiles\datacentre_api` and `C:\WorkFiles\datacentre_updater`; build the `${DATACENTRE_API_IMAGE}` / `${DATACENTRE_UPDATER_IMAGE}` images directly from there (see "Building images" below). The `datacentre_updater` upstream Dockerfile must include the build-time `utf8mb4_0900_bin` → `utf8mb4_bin` sed patch on `db_manager.py`; if your image doesn't have it, the compose entrypoint applies it at runtime as a fallback.

`subgraphs/{stats,sv,democrit}/` ARE vendored because they're bind-mounted into `subgraph-deploy` at runtime (`graph codegen` + `graph build` run inside the container against these files). Re-sync from upstream (`C:\WorkFiles\stats-subgraph`, `C:\WorkFiles\subgraph`, `C:\WorkFiles\democrit-evm\subgraph`) is a manual `cp -r`.

### Internal networking

Services reference each other by Docker hostname (`http://nginx/chain`, `http://graph-node:8000/subgraphs/name/sv`, `mysql://...@mariadb:3306/...`, `redis://redis:6379`, `graph-postgres:5432`, `ipfs:5001`). From the host, `nginx` (port 8100), `mariadb` (`${MARIADB_PORT}`, default 3307), `redis` (`${REDIS_PORT}`, default 6380 on the host — alt port to avoid colliding with any pre-existing redis on the host; the container internally still listens on 6379), and `grafana` (`${GRAFANA_PORT}`, default 3000) are reachable. Redis is published primarily to let a frontend running outside the Docker network share this stack's cache layer; it has no auth, so treat the LAN as trusted.

### Boot timing

Cold boot from `docker compose up -d`:
- ~30s — graph-node ready
- ~1-2m — `subgraph-deploy` finishes and writes `graph_hashes.env`
- ~2-30m — `mariadb` healthy (first-boot import depends on dump size)
- ~2m+ after the above — `datacentre-api` starts; MariaDB+SQLite endpoints work fully
- hours-days — graph-node indexing catches up; subgraph-backed endpoints (`/leaderboards`, `/user_activity`, `/shop`, parts of `/market`) progressively populate

Indexing volume can be substantial — graph-node will issue many `eth_getLogs`/`eth_call` requests to `BLOCKCHAIN_RPC_URL` during cold sync.

## Configuration

Copy `.env.example` to `.env`. Required:

- `BLOCKCHAIN_RPC_URL` — full RPC URL (used by both nginx and graph-node)
- `BLOCKCHAIN_AUTH_HEADER` — `Authorization` header value (empty if endpoint needs none)
- `ACCOUNTS_CONTRACT` — XayaAccounts address
- `GSP_IMAGE` — Soccerverse GSP image
- `DATACENTRE_API_IMAGE` — pre-built image for the API
- `DATACENTRE_UPDATER_IMAGE` — pre-built image for the updater
- `SUBGRAPH_DEPLOY_IMAGE` — pre-built image for the one-shot subgraph deployer

Optional:

- `GAME_ID` (default `sv`)
- `STORAGE_SQLITE_PATH`, `ARCHIVAL_SQL_PATH`, `USERCONFIG_SQL_PATH` — first-boot seed inputs
- `DEMOCRIT_ABI_PATH`, `VAULTMANAGER_ABI_PATH` — host paths to real democrit ABI JSONs (forge build artefacts). When unset, in-repo placeholders are used and `subgraph-deploy` skips democrit. See `subgraphs/democrit/POPULATE_ABIS.md`.
- `MARIADB_PORT` (default `3307`) — host-side port

All other configuration (Redis URL, Graph Postgres credentials, BigQuery local DB, dump folder paths, subgraph URLs, etc.) is hardcoded in `docker-compose.yml` because those are stack-internal Docker hostnames.

## Building images

Two of the three image sources live in separate upstream repos; one (`subgraph-deploy`) is in this repo.

```bash
# datacentre-api — built from the upstream repo (NOT vendored here)
cd /c/WorkFiles/datacentre_api
docker build -t YOUR_REGISTRY/datacentre-api:VERSION \
  -f docker/Dockerfile .
docker push YOUR_REGISTRY/datacentre-api:VERSION

# datacentre-updater — built from the upstream repo (NOT vendored here)
cd /c/WorkFiles/datacentre_updater
docker build -t YOUR_REGISTRY/datacentre-updater:VERSION \
  -f docker/Dockerfile .
docker push YOUR_REGISTRY/datacentre-updater:VERSION

# subgraph-deploy — built from this repo. Build context MUST be the repo
# root (not subgraph-deploy/) because the subgraph sources are baked into
# the image (so the stack works on Portainer agents that don't clone the
# repo to the host filesystem).
cd /c/WorkFiles/forked-evm-testing
docker build -f subgraph-deploy/Dockerfile -t YOUR_REGISTRY/subgraph-deploy:VERSION .
docker push YOUR_REGISTRY/subgraph-deploy:VERSION
```

When subgraph sources change (`subgraphs/{stats,sv,democrit}/`), rebuild and push the `subgraph-deploy` image — there is no longer a bind mount, so changes only take effect after a new image is pulled.

Then set the matching `*_IMAGE` env vars in `.env`. Re-running `docker compose up -d` pulls the new images.

## Operational notes

- **First start is slow.** `mariadb` archival import + graph-node indexing both take time. Healthcheck `start_period` for mariadb is 30 minutes; for graph-node it's just service-up (indexing happens in the background).
- **`gsp-init` schema migrations are idempotent** (`2>/dev/null || true`). Add new ALTERs in the same pattern.
- **Re-seeding the GSP SQLite snapshot.** `gsp-init` only seeds when `gspdata:/xayagame/sv/polygon/storage.sqlite` is missing — and the named `gspdata` volume survives `docker compose down` and most Portainer redeploys. To force a re-seed from `${STORAGE_SQLITE_PATH}` without manually wiping the volume, set `FORCE_RESEED=1` in `.env` and recreate the affected services: `FORCE_RESEED=1 docker compose up -d --force-recreate gsp-init gsp`. Flip back to `0` afterwards so subsequent restarts don't keep clobbering live GSP state. (Wiping the volume — `docker volume rm <stack>_gspdata` after `docker compose down` — is the equivalent manual path.)
- **`subgraph-deploy` is idempotent.** Re-running on unchanged sources is fast — graph-cli detects unchanged manifests. The deploy script overwrites `/shared/graph_hashes.env` each run.
- **Subgraph sources are baked into the image.** They are NOT bind-mounted from the host. This is intentional: Portainer agents in some configurations don't clone the git repo to the host, so bind mounts pointing at `./subgraphs/...` resolve to empty dirs (Docker silently auto-creates missing bind sources). To update subgraph sources, rebuild and push `${SUBGRAPH_DEPLOY_IMAGE}` per the "Building images" section.
- **democrit subgraph ships with placeholder ABIs baked in.** Real ABIs require `forge build` of the upstream `democrit-evm` repo. Operator supplies host paths via `DEMOCRIT_ABI_PATH` / `VAULTMANAGER_ABI_PATH`; deploy.sh copies them over the placeholders at runtime. When unset, the placeholders stay in place and democrit is skipped (logs a warning); the stack still boots cleanly. See `subgraphs/democrit/POPULATE_ABIS.md`.
- **Collation patch:** `datacentre-updater`'s compose entrypoint runs `sed -i 's/utf8mb4_0900_bin/utf8mb4_bin/g' db_manager.py` at every container start (idempotent — once patched, sed matches nothing). The vendored `datacentre_updater/docker/Dockerfile` also applies the patch at build time, so anyone re-rolling the image from source gets it baked in.
- **Editing `proxy.conf.template`:** the file is rendered through `envsubst` with the explicit allowlist `$BLOCKCHAIN_RPC_URL $BLOCKCHAIN_AUTH_HEADER` in `nginx/entrypoint.sh`. Any new env var referenced in the template must be added to that allowlist.
- **Stack memory footprint:** ~6-8 GB RAM during indexing. Postgres + graph-node + mariadb dominate.
- **Legacy directories**: `basechain/` and `helper/` remain in the tree from the original Anvil-fork design but are no longer wired into compose. Treat as inert unless re-introduced.

## Monitoring

- **Open the dashboard:** `http://localhost:${GRAFANA_PORT:-3000}` → log in with `${GRAFANA_ADMIN_USER}` / `${GRAFANA_ADMIN_PASSWORD}` → "API Performance" folder → "API Performance" dashboard.
- **No data yet?** Plan 1's frontend recorder needs to be deployed and a browser session must have hit a page to populate `api_request_log`. Row 1 (Prom) panels populate as soon as the beacon endpoint receives any event; Rows 2–4 (MariaDB) need ~1 minute of writes.
- **Edit the dashboard live, save back to git:** Grafana → dashboard → Settings (gear) → JSON Model → copy the JSON → overwrite `monitoring/grafana/dashboards/api-perf.json` → `docker compose restart grafana` to confirm the file load works → commit.
- **Wipe history:** `docker compose down -v` removes both `prometheus_data` and `grafana_data` (along with everything else).
- **Memory footprint:** Prometheus ~200 MB RSS, Grafana ~150 MB. Total stack roughly 6.5–8.5 GB during indexing.

## Privacy

The API performance monitoring captures only operational telemetry — never user content. Each event records: an anonymous browser-session UUID generated client-side and stored in `localStorage` (no link to any real user), the Next.js page-route template, the outbound URL (with query strings and basic-auth userinfo stripped server-side), HTTP method/status, response duration, response `Content-Length`, and a 200-character truncated error message on network failures. The recorder does NOT read request bodies, request headers (including `Authorization`), cookies, response bodies, wallet addresses, or Xaya names. The originating IP is used by the beacon endpoint for rate-limiting only; it is not stored. Retention in MariaDB is 270 days, after which daily partitions are dropped automatically. This data is treated as internal operational telemetry and is not exported. If the public privacy policy enumerates collected data, add an entry mentioning operational performance telemetry consistent with this paragraph.

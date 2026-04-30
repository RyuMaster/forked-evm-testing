# Datacentre Stack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Vendor `datacentre_api` and `datacentre_updater` into the stack, build them locally, and add a complete Graph Protocol indexing stack (graph-node + ipfs + postgres + 3 subgraphs) plus Redis so every API endpoint can be exercised against a self-contained local environment.

**Architecture:** Single `docker-compose.yml` extension. Vendored copies of two Python services and three subgraph projects sit as siblings of the existing services. A new one-shot `subgraph-deploy` container deploys the three subgraphs to graph-node at boot and writes their locally-assigned IPFS hashes to a shared volume; `datacentre-api`'s entrypoint sources that file so its `lookup_subgraph_schemas()` resolves correctly.

**Tech Stack:** Docker Compose, Docker BuildKit, Python 3.13/3.x (FastAPI/Gunicorn for API; raw Python for updater), MariaDB 11, Redis 7, PostgreSQL 14, IPFS Kubo, graph-node, graph-cli (Node 20).

**Spec:** `docs/superpowers/specs/2026-04-30-datacentre-stack-design.md`

---

## File Map

**New (vendored, copied wholesale):**
- `datacentre_api/` — FastAPI service source
- `datacentre_updater/` — Python script + scheduler source
- `subgraphs/stats/` — xaya-stats subgraph
- `subgraphs/sv/` — sv-subgraph
- `subgraphs/democrit/` — democrit-sv subgraph

**New (authored):**
- `subgraph-deploy/Dockerfile`
- `subgraph-deploy/deploy.sh`

**Modified:**
- `docker-compose.yml` — add 5 services, modify 3, add 5 volumes
- `db-init/init-databases.sh` — one extra `CREATE DATABASE`
- `datacentre_updater/docker/Dockerfile` — one-line `sed` patch
- `.env.example` — drop image vars, add `GAME_ID`
- `CLAUDE.md` — refresh to reflect new topology

---

## Conventions used in this plan

- All `cd` happens implicitly — paths are absolute or relative to repo root `C:/WorkFiles/forked-evm-testing/`.
- Shell snippets assume bash (Git Bash on Windows). The `cp -r` calls use forward slashes.
- After every task: `git add` only the listed files, then commit with the listed message. Never `git add -A`.
- "Verify" steps run real Docker commands. They're the closest thing to a unit test for compose work.

---

### Task 1: Vendor source trees

**Files:**
- Create: `datacentre_api/` (full tree)
- Create: `datacentre_updater/` (full tree)
- Create: `subgraphs/stats/` (full tree)
- Create: `subgraphs/sv/` (full tree)
- Create: `subgraphs/democrit/` (full tree)

- [ ] **Step 1: Copy the two service trees**

```bash
cp -r /c/WorkFiles/datacentre_api    /c/WorkFiles/forked-evm-testing/datacentre_api
cp -r /c/WorkFiles/datacentre_updater /c/WorkFiles/forked-evm-testing/datacentre_updater
```

- [ ] **Step 2: Copy the three subgraph trees into a single subfolder**

```bash
mkdir -p /c/WorkFiles/forked-evm-testing/subgraphs
cp -r /c/WorkFiles/stats-subgraph        /c/WorkFiles/forked-evm-testing/subgraphs/stats
cp -r /c/WorkFiles/subgraph              /c/WorkFiles/forked-evm-testing/subgraphs/sv
cp -r /c/WorkFiles/democrit-evm/subgraph /c/WorkFiles/forked-evm-testing/subgraphs/democrit
```

Note: the democrit subgraph is a *subfolder* of the `democrit-evm` repo. We vendor only the `subgraph/` subdir to keep the three-sibling layout consistent with stats and sv.

- [ ] **Step 3: Strip any pre-existing build artefacts**

```bash
cd /c/WorkFiles/forked-evm-testing
rm -rf datacentre_api/__pycache__ datacentre_api/.pytest_cache
rm -rf datacentre_updater/__pycache__
find subgraphs -type d \( -name node_modules -o -name build -o -name generated \) -prune -exec rm -rf {} +
```

`graph codegen` and `graph build` regenerate `generated/` and `build/` at deploy time; vendoring them creates noise in git diffs.

- [ ] **Step 4: Verify each tree has its expected entry points**

```bash
ls datacentre_api/main.py datacentre_api/docker/Dockerfile datacentre_api/requirements.txt
ls datacentre_updater/main.py datacentre_updater/docker/Dockerfile datacentre_updater/db_manager.py
ls subgraphs/stats/subgraph.yaml subgraphs/stats/package.json
ls subgraphs/sv/subgraph.yaml subgraphs/sv/package.json
ls subgraphs/democrit/subgraph.yaml subgraphs/democrit/package.json
```

Expected: all paths exist (no `cannot access` errors).

- [ ] **Step 5: Commit**

```bash
git add datacentre_api datacentre_updater subgraphs
git commit -m "Vendor datacentre_api, datacentre_updater, and three subgraphs"
```

This commit will be large (hundreds of files). That's expected — it's a one-time vendor.

---

### Task 2: Patch the vendored datacentre_updater Dockerfile

**Files:**
- Modify: `datacentre_updater/docker/Dockerfile`

The current `docker-compose.yml` works around the `utf8mb4_0900_bin` collation issue at runtime via a `sed` invocation in the entrypoint. Since we now own the vendored copy, move the patch to build time so the runtime entrypoint can be the Dockerfile default.

- [ ] **Step 1: Add the sed patch after `COPY . .`**

Edit `datacentre_updater/docker/Dockerfile`. Find this block:

```dockerfile
# Copy rest of the files
COPY --chown=python-user:python-user . .

# Update perms and switch user to non-root before starting the app
```

Insert these two lines between `COPY` and `Update perms`:

```dockerfile
# Patch upstream collation that MariaDB rejects.
# See docs/superpowers/specs/2026-04-30-datacentre-stack-design.md
RUN sed -i 's/utf8mb4_0900_bin/utf8mb4_bin/g' db_manager.py
```

Final block should read:

```dockerfile
# Copy rest of the files
COPY --chown=python-user:python-user . .

# Patch upstream collation that MariaDB rejects.
# See docs/superpowers/specs/2026-04-30-datacentre-stack-design.md
RUN sed -i 's/utf8mb4_0900_bin/utf8mb4_bin/g' db_manager.py

# Update perms and switch user to non-root before starting the app
```

- [ ] **Step 2: Verify the patch survives a build**

```bash
cd /c/WorkFiles/forked-evm-testing
docker build -t test-updater-patch -f datacentre_updater/docker/Dockerfile datacentre_updater
docker run --rm test-updater-patch grep -c utf8mb4_0900_bin db_manager.py
```

Expected: `0` (no occurrences left). Then clean up:

```bash
docker rmi test-updater-patch
```

- [ ] **Step 3: Commit**

```bash
git add datacentre_updater/docker/Dockerfile
git commit -m "Apply utf8mb4 collation patch at build time, not runtime"
```

---

### Task 3: Add `bigquery_local` database to mariadb init

**Files:**
- Modify: `db-init/init-databases.sh`

- [ ] **Step 1: Add the database creation**

Edit `db-init/init-databases.sh`. Find:

```bash
mariadb -u root -p"${MARIADB_ROOT_PASSWORD}" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS datacentre;
    CREATE DATABASE IF NOT EXISTS userconfig;
    CREATE DATABASE IF NOT EXISTS lapsed_users;
EOSQL
```

Replace with:

```bash
mariadb -u root -p"${MARIADB_ROOT_PASSWORD}" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS datacentre;
    CREATE DATABASE IF NOT EXISTS userconfig;
    CREATE DATABASE IF NOT EXISTS lapsed_users;
    CREATE DATABASE IF NOT EXISTS bigquery_local;
EOSQL
```

- [ ] **Step 2: Verify mariadb image rebuilds cleanly**

```bash
docker compose build mariadb
```

Expected: build succeeds. (We can't actually verify the new DB exists without nuking the existing `mariadb_data` volume, which we won't do here — Task 15 covers full cold-boot validation.)

- [ ] **Step 3: Commit**

```bash
git add db-init/init-databases.sh
git commit -m "Add bigquery_local database to mariadb init"
```

---

### Task 4: Switch `datacentre-updater` to local build with full env

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Replace the `datacentre-updater` service block**

Edit `docker-compose.yml`. Find the existing block (currently lines ~116-137):

```yaml
  datacentre-updater:
    image: ${DATACENTRE_UPDATER_IMAGE}
    entrypoint: ["/bin/bash", "-c", "sed -i \"s/utf8mb4_0900_bin/utf8mb4_bin/g\" db_manager.py && python main.py"]
    environment:
      SOURCE_DB_HOST: mariadb
      SOURCE_DB_USER: root
      SOURCE_DB_PASSWORD: gsparchival
      SOURCE_DB_NAME: archival
      DEST_DB_HOST: mariadb
      DEST_DB_USER: root
      DEST_DB_PASSWORD: gsparchival
      DEST_DB_NAME: datacentre
      SQLITE_PATH: /xayagame/sv/polygon/storage.sqlite
      GAME_ID: sv
    volumes:
      - gspdata:/xayagame:ro
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
    restart: on-failure
```

Replace it with:

```yaml
  datacentre-updater:
    build:
      context: ./datacentre_updater
      dockerfile: docker/Dockerfile
    environment:
      SOURCE_DB_HOST: mariadb
      SOURCE_DB_USER: root
      SOURCE_DB_PASSWORD: gsparchival
      SOURCE_DB_NAME: archival
      DEST_DB_HOST: mariadb
      DEST_DB_USER: root
      DEST_DB_PASSWORD: gsparchival
      DEST_DB_NAME: datacentre
      BIGQUERY_DB_HOST: mariadb
      BIGQUERY_DB_USER: root
      BIGQUERY_DB_PASSWORD: gsparchival
      BIGQUERY_DB_NAME: bigquery_local
      SQLITE_PATH: /xayagame/sv/polygon/storage.sqlite
      DATAPACK_URL: https://downloads.soccerverse.com/svpack/default.json
      DUMP_OUTPUT_FOLDER: /dumps
      GAME_ID: ${GAME_ID:-sv}
      SVC_POLYGON_SUBGRAPH_URL: http://graph-node:8000/subgraphs/name/democrit
      POLYGON_STATS_SUBGRAPH_URL: http://graph-node:8000/subgraphs/name/stats
      SV_SUBGRAPH_URL: http://graph-node:8000/subgraphs/name/sv
    volumes:
      - gspdata:/xayagame:ro
      - datadumps:/dumps
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
    restart: on-failure
```

Two changes vs old: `image:` → `build:` (with explicit Dockerfile path), `entrypoint:` removed (the build-time patch from Task 2 makes the runtime sed obsolete; CMD from the Dockerfile now runs by default), and seven new env vars covering BigQuery, dumps, subgraph URLs, and DataPack URL.

We do **not** add a `subgraph-deploy` dependency yet — that service is introduced in Task 11. We will add the `depends_on` then.

- [ ] **Step 2: Add the `datadumps` volume**

Find the `volumes:` block at the bottom:

```yaml
volumes:
  gspdata:
  mariadb_data:
```

Replace with:

```yaml
volumes:
  gspdata:
  mariadb_data:
  datadumps:
```

- [ ] **Step 3: Verify the service builds**

```bash
docker compose build datacentre-updater
```

Expected: builds successfully. The image will be tagged `forked-evm-testing-datacentre-updater` (or similar — Compose names it from the project + service).

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "Switch datacentre-updater to local build, add BigQuery+subgraph+dump env"
```

---

### Task 5: Switch `datacentre-api` to local build with new env

**Files:**
- Modify: `docker-compose.yml`

This task adds Redis/Graph/dumps env vars but **does not yet** wire the entrypoint to source `/shared/graph_hashes.env`. That happens in Task 12 once `subgraph-deploy` exists. For now, `GRAPH_SUBGRAPH_STATS` and `GRAPH_SUBGRAPH_SV` are simply unset, which causes the API to log "user activity features will be disabled" at startup — same graceful-degrade behaviour as today.

- [ ] **Step 1: Replace the `datacentre-api` service block**

Find the existing block:

```yaml
  datacentre-api:
    image: ${DATACENTRE_API_IMAGE}
    environment:
      MYSQL_HOST: mariadb
      MYSQL_PORT: "3306"
      MYSQL_USER: root
      MYSQL_PASSWORD: gsparchival
      MYSQL_DB: datacentre
      MYSQL_ARCHIVAL_HOST: mariadb
      MYSQL_ARCHIVAL_PORT: "3306"
      MYSQL_ARCHIVAL_USER: root
      MYSQL_ARCHIVAL_PASSWORD: gsparchival
      MYSQL_ARCHIVAL_DB: archival
      USERCONFIG_HOST: mariadb
      USERCONFIG_PORT: "3306"
      USERCONFIG_USER: root
      USERCONFIG_PASS: gsparchival
      USERCONFIG_DB: userconfig
      SQLITE_DB_PATH: /xayagame/sv/polygon/storage.sqlite
      ROOT_PATH: /api
      GAME_ID: sv
    volumes:
      - gspdata:/xayagame:ro
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
    restart: on-failure
```

Replace with:

```yaml
  datacentre-api:
    build:
      context: ./datacentre_api
      dockerfile: docker/Dockerfile
    environment:
      MYSQL_HOST: mariadb
      MYSQL_PORT: "3306"
      MYSQL_USER: root
      MYSQL_PASSWORD: gsparchival
      MYSQL_DB: datacentre
      MYSQL_ARCHIVAL_HOST: mariadb
      MYSQL_ARCHIVAL_PORT: "3306"
      MYSQL_ARCHIVAL_USER: root
      MYSQL_ARCHIVAL_PASSWORD: gsparchival
      MYSQL_ARCHIVAL_DB: archival
      USERCONFIG_HOST: mariadb
      USERCONFIG_PORT: "3306"
      USERCONFIG_USER: root
      USERCONFIG_PASS: gsparchival
      USERCONFIG_DB: userconfig
      SQLITE_DB_PATH: /xayagame/sv/polygon/storage.sqlite
      ROOT_PATH: /api
      GAME_ID: ${GAME_ID:-sv}
      REDIS_HOST: redis
      REDIS_PORT: "6379"
      REDIS_DB: "0"
      REDIS_PASSWORD: ""
      GRAPH_POSTGRES_HOST: graph-postgres
      GRAPH_POSTGRES_PORT: "5432"
      GRAPH_POSTGRES_USER: graph-node
      GRAPH_POSTGRES_PASSWORD: graph-node
      GRAPH_POSTGRES_DB: graph-node
      GRAPH_SUBGRAPH_SV_URL: http://graph-node:8000/subgraphs/name/sv
      DEFAULT_PROFILE_PIC_URL: https://downloads.soccerverse.com/default_profile.jpg
      DUMP_OUTPUT_FOLDER: /dumps
    volumes:
      - gspdata:/xayagame:ro
      - datadumps:/dumps:ro
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
    restart: on-failure
```

- [ ] **Step 2: Verify the service builds**

```bash
docker compose build datacentre-api
```

Expected: builds successfully. Note the API Dockerfile builds `data/player_history.sqlite` from `data/player_history.sql` at image build time — that runs as part of this build.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "Switch datacentre-api to local build, add Redis+Graph+dump env"
```

---

### Task 6: Add Redis service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add the `redis` service**

Insert this block in `docker-compose.yml` (anywhere among the services; alphabetical order suggests near `nginx`/`mariadb`):

```yaml
  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    volumes:
      - redis_data:/data
    stop_grace_period: 5s
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
```

- [ ] **Step 2: Add the `redis_data` volume**

In the `volumes:` block at the bottom, add `redis_data:`:

```yaml
volumes:
  gspdata:
  mariadb_data:
  datadumps:
  redis_data:
```

- [ ] **Step 3: Add `redis: service_healthy` to the `datacentre-api` `depends_on`**

In `datacentre-api`'s `depends_on`, append:

```yaml
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
```

- [ ] **Step 4: Verify Redis boots and healthchecks pass**

```bash
docker compose up -d redis
docker compose ps redis
```

Expected: status `running (healthy)` (may take ~10s). Then:

```bash
docker compose exec redis redis-cli ping
```

Expected: `PONG`.

Tear down:

```bash
docker compose down
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "Add Redis service for datacentre-api caching"
```

---

### Task 7: Add `graph-postgres` service

**Files:**
- Modify: `docker-compose.yml`

graph-node requires PostgreSQL with locale `C` and UTF8 encoding (per its docs). It will create its own `public.deployment_schemas` table at first start.

- [ ] **Step 1: Add the `graph-postgres` service**

Insert in `docker-compose.yml`:

```yaml
  graph-postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: graph-node
      POSTGRES_PASSWORD: graph-node
      POSTGRES_DB: graph-node
      POSTGRES_INITDB_ARGS: "--locale=C --encoding=UTF8"
    volumes:
      - graph_postgres_data:/var/lib/postgresql/data
    stop_grace_period: 30s
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U graph-node -d graph-node"]
      interval: 5s
      timeout: 5s
      retries: 10
```

- [ ] **Step 2: Add the volume**

```yaml
volumes:
  gspdata:
  mariadb_data:
  datadumps:
  redis_data:
  graph_postgres_data:
```

- [ ] **Step 3: Add `graph-postgres: service_healthy` to `datacentre-api` `depends_on`**

```yaml
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
      graph-postgres:
        condition: service_healthy
```

- [ ] **Step 4: Verify boot and healthcheck**

```bash
docker compose up -d graph-postgres
docker compose ps graph-postgres
```

Expected: `running (healthy)` after ~5-10s.

```bash
docker compose exec graph-postgres psql -U graph-node -d graph-node -c "SELECT 1;"
```

Expected: returns `1`.

Tear down:

```bash
docker compose down
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "Add graph-postgres service for graph-node storage"
```

---

### Task 8: Add IPFS service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add the `ipfs` service**

```yaml
  ipfs:
    image: ipfs/kubo:latest
    volumes:
      - ipfs_data:/data/ipfs
    stop_grace_period: 10s
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 20s
```

- [ ] **Step 2: Add the volume**

```yaml
volumes:
  gspdata:
  mariadb_data:
  datadumps:
  redis_data:
  graph_postgres_data:
  ipfs_data:
```

- [ ] **Step 3: Verify boot**

```bash
docker compose up -d ipfs
docker compose ps ipfs
```

Expected: `running (healthy)` within ~30s.

```bash
docker compose exec ipfs ipfs id
```

Expected: a JSON blob starting with `{"ID": "12D3KooW..."`.

```bash
docker compose down
```

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "Add IPFS service for subgraph manifest storage"
```

---

### Task 9: Add graph-node service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add the `graph-node` service**

```yaml
  graph-node:
    image: graphprotocol/graph-node:latest
    environment:
      postgres_host: graph-postgres
      postgres_user: graph-node
      postgres_pass: graph-node
      postgres_db: graph-node
      ipfs: "ipfs:5001"
      ethereum: "matic:http://nginx/chain"
      GRAPH_LOG: info
      RUST_LOG: info
    stop_grace_period: 10s
    depends_on:
      graph-postgres:
        condition: service_healthy
      ipfs:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8030/graphql -H 'content-type: application/json' --post-data '{\"query\":\"{indexingStatuses{subgraph}}\"}' >/dev/null 2>&1 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 30s
```

The healthcheck hits the index-node GraphQL on port 8030. We use `wget` because `curl` isn't installed in the graph-node image. The query is harmless (returns empty list when nothing is deployed). 30 retries × 10s = up to 5 minutes for cold start, which covers the slow first-boot when graph-node runs Postgres migrations.

- [ ] **Step 2: Verify graph-node boots**

```bash
docker compose up -d graph-node
docker compose logs -f graph-node
```

Watch for the line `Started all subgraphs` or `Index node server listening`. Press Ctrl+C to detach.

```bash
docker compose ps graph-node
```

Expected: `running (healthy)` within ~1 minute. If it stays unhealthy, check logs for connection errors to graph-postgres or ipfs.

```bash
docker compose down
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "Add graph-node service indexing matic via /chain proxy"
```

---

### Task 10: Create `subgraph-deploy` build context

**Files:**
- Create: `subgraph-deploy/Dockerfile`
- Create: `subgraph-deploy/deploy.sh`

- [ ] **Step 1: Create the directory**

```bash
mkdir -p /c/WorkFiles/forked-evm-testing/subgraph-deploy
```

- [ ] **Step 2: Write `subgraph-deploy/Dockerfile`**

Create `subgraph-deploy/Dockerfile`:

```dockerfile
FROM node:20-alpine

# psql lets us read the IPFS hash directly from graph-node's deployment_schemas
# table (more robust than parsing graph-cli stdout).
RUN apk add --no-cache postgresql-client

# Pin graph-cli. Keep aligned with subgraphs' devDependency.
RUN npm install -g @graphprotocol/graph-cli@0.98.1

WORKDIR /work
COPY deploy.sh /usr/local/bin/deploy.sh
RUN chmod +x /usr/local/bin/deploy.sh

ENTRYPOINT ["/usr/local/bin/deploy.sh"]
```

- [ ] **Step 3: Write `subgraph-deploy/deploy.sh`**

Create `subgraph-deploy/deploy.sh`:

```sh
#!/bin/sh
# Deploy the three vendored subgraphs to graph-node and record the
# locally-assigned IPFS hashes to a shared volume so datacentre-api can
# resolve them at runtime.
set -eu

GRAPH_ADMIN_URL="http://graph-node:8020"
IPFS_URL="http://ipfs:5001"
SHARED_DIR="/shared"
HASH_FILE="${SHARED_DIR}/graph_hashes.env"

mkdir -p "${SHARED_DIR}"
: > "${HASH_FILE}"

export PGPASSWORD=graph-node
PG="psql -h graph-postgres -U graph-node -d graph-node -tAq"

deploy_one() {
  name=$1
  src=$2
  envvar=$3

  echo "=== Deploying ${name} from ${src} ==="

  # Skip if any ABI file (in either abi/ or abis/) contains the placeholder marker.
  # This lets the democrit subgraph ship with placeholder ABIs (see
  # subgraphs/democrit/POPULATE_ABIS.md) without breaking the rest of the stack.
  # Subgraph layouts vary: stats uses abi/, sv and democrit use abis/.
  if grep -lq _DATACENTRE_STACK_PLACEHOLDER "${src}/abi/"*.json "${src}/abis/"*.json 2>/dev/null; then
    echo "WARNING: ${name} has placeholder ABIs — skipping deploy."
    echo "         See ${src}/POPULATE_ABIS.md to populate real ABIs."
    return 0
  fi

  # Subgraph dirs are mounted read-only; copy to writable scratch.
  rm -rf "/work/${name}"
  cp -r "${src}" "/work/${name}"
  cd "/work/${name}"

  npm install --omit=dev --no-audit --no-fund

  npx graph codegen
  npx graph build

  # Idempotent — graph create exits non-zero if the subgraph already exists.
  # Swallow only that specific error; let any other failure abort.
  if ! create_err=$(npx graph create --node "${GRAPH_ADMIN_URL}" "${name}" 2>&1); then
    case "${create_err}" in
      *"already exists"*|*"name not available"*)
        echo "graph create: ${name} already exists, continuing."
        ;;
      *)
        echo "graph create failed for ${name}:" >&2
        echo "${create_err}" >&2
        exit 1
        ;;
    esac
  fi

  npx graph deploy \
    --node "${GRAPH_ADMIN_URL}" \
    --ipfs "${IPFS_URL}" \
    --version-label v1 \
    "${name}"

  # Resolve the deployment IPFS hash from graph-node's bookkeeping tables.
  # subgraphs.subgraph.current_version → subgraphs.subgraph_version.deployment
  # (the deployment column is the IPFS hash). This is graph-node's own ground
  # truth, stable across graph-cli versions.
  hash=$(${PG} -c "SELECT sv.deployment FROM subgraphs.subgraph s JOIN subgraphs.subgraph_version sv ON s.current_version = sv.id WHERE s.name = '${name}' ORDER BY sv.created_at DESC LIMIT 1;")

  if [ -z "${hash}" ]; then
    echo "ERROR: could not resolve IPFS hash for ${name}" >&2
    echo "Dumping subgraph state for debugging:" >&2
    ${PG} -c "SELECT name, current_version FROM subgraphs.subgraph;" >&2 || true
    ${PG} -c "SELECT id, name, subgraph FROM public.deployment_schemas;" >&2 || true
    exit 1
  fi

  echo "${envvar}=${hash}" >> "${HASH_FILE}"
  echo "Recorded ${envvar}=${hash}"
}

deploy_one stats    /subgraphs/stats    GRAPH_SUBGRAPH_STATS
deploy_one sv       /subgraphs/sv       GRAPH_SUBGRAPH_SV
deploy_one democrit /subgraphs/democrit GRAPH_SUBGRAPH_DEMOCRIT

echo
echo "=== All subgraphs deployed. Wrote ${HASH_FILE}: ==="
cat "${HASH_FILE}"
```

The hash-resolution query maps a CLI subgraph name ("stats" / "sv" / "democrit") to its current deployment IPFS hash via `subgraphs.subgraph` → `subgraphs.subgraph_version`. This is graph-node's own ground truth and survives graph-cli version drift. If the query returns empty, the script dumps the subgraph state to stderr and exits non-zero — better than silently writing garbage hashes.

- [ ] **Step 4: Make it executable on disk and verify the build**

```bash
chmod +x /c/WorkFiles/forked-evm-testing/subgraph-deploy/deploy.sh
docker build -t test-subgraph-deploy /c/WorkFiles/forked-evm-testing/subgraph-deploy
docker run --rm --entrypoint /usr/local/bin/deploy.sh test-subgraph-deploy --help 2>&1 | head -5 || true
```

The image builds; the script doesn't accept `--help` so it'll just exit on missing services (expected at this stage). Clean up:

```bash
docker rmi test-subgraph-deploy
```

- [ ] **Step 5: Commit**

```bash
git add subgraph-deploy
git commit -m "Add subgraph-deploy build context for graph-cli one-shot"
```

---

### Task 11: Wire `subgraph-deploy` as a one-shot service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add the `subgraph-deploy` service**

Insert into `docker-compose.yml`:

```yaml
  subgraph-deploy:
    build:
      context: ./subgraph-deploy
    volumes:
      - ./subgraphs/stats:/subgraphs/stats:ro
      - ./subgraphs/sv:/subgraphs/sv:ro
      - ./subgraphs/democrit:/subgraphs/democrit:ro
      - graph_hashes_shared:/shared
    depends_on:
      graph-node:
        condition: service_healthy
    restart: "no"
```

`restart: "no"` ensures it runs once. On re-`up` the deploy script is idempotent: graph-cli skips already-deployed unchanged subgraphs and overwrites the env file each run.

- [ ] **Step 2: Add the `graph_hashes_shared` volume**

```yaml
volumes:
  gspdata:
  mariadb_data:
  datadumps:
  redis_data:
  graph_postgres_data:
  ipfs_data:
  graph_hashes_shared:
```

- [ ] **Step 3: Add `subgraph-deploy: service_completed_successfully` to `datacentre-updater` `depends_on`**

```yaml
  datacentre-updater:
    ...
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
      subgraph-deploy:
        condition: service_completed_successfully
```

- [ ] **Step 4: Verify the deploy succeeds end-to-end**

This task is the first that requires graph-postgres + ipfs + graph-node + nginx (for the chain RPC) to all be up. Make sure your `.env` has `BLOCKCHAIN_RPC_URL` and `BLOCKCHAIN_AUTH_HEADER` set.

```bash
docker compose up -d nginx graph-postgres ipfs graph-node
docker compose run --rm subgraph-deploy
```

Expected: each of the three subgraphs deploys; final output shows three `Recorded GRAPH_SUBGRAPH_*=Qm...` lines. If `npm install` fails for any subgraph because of a missing peer dependency, check the subgraph's own `package.json` against the graph-cli version pinned in `subgraph-deploy/Dockerfile` (currently `0.98.1`).

If deploy succeeds, inspect the shared volume:

```bash
docker compose run --rm --entrypoint cat subgraph-deploy /shared/graph_hashes.env
```

Expected: three `GRAPH_SUBGRAPH_*=Qm...` lines.

Tear down:

```bash
docker compose down
```

(Don't `--volumes` — keep the indexing progress to save time on the integration test.)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "Wire subgraph-deploy one-shot, share hashes via volume"
```

---

### Task 12: Wire `graph_hashes_shared` into datacentre-api

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Mount the shared volume read-only into datacentre-api**

In the `datacentre-api` block, change `volumes:` from:

```yaml
    volumes:
      - gspdata:/xayagame:ro
      - datadumps:/dumps:ro
```

to:

```yaml
    volumes:
      - gspdata:/xayagame:ro
      - datadumps:/dumps:ro
      - graph_hashes_shared:/shared:ro
```

- [ ] **Step 2: Add the entrypoint wrapper**

In the `datacentre-api` block, add an `entrypoint:` key just below `build:`:

```yaml
  datacentre-api:
    build:
      context: ./datacentre_api
      dockerfile: docker/Dockerfile
    entrypoint: ["sh", "-c", "set -a; . /shared/graph_hashes.env; set +a; exec python3 main.py"]
    environment:
      ...
```

The `set -a` / `set +a` brackets cause every assignment in the sourced file to be auto-exported, so Python's `os.getenv()` sees them. A bare `.` would leave them as shell-local only.

- [ ] **Step 3: Add `subgraph-deploy: service_completed_successfully` to `datacentre-api` `depends_on`**

```yaml
    depends_on:
      mariadb:
        condition: service_healthy
      gsp-init:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
      graph-postgres:
        condition: service_healthy
      subgraph-deploy:
        condition: service_completed_successfully
```

- [ ] **Step 4: Verify the API boots and resolves schemas**

```bash
docker compose up -d --build datacentre-api
docker compose logs datacentre-api | grep -E "Resolved IPFS hash|Could not find schema|user activity"
```

Expected: two log lines like `Resolved IPFS hash QmXXX to schema sgdN` (one for stats, one for sv). If you see `Could not find schema for IPFS hash...`, the entrypoint sourcing failed — check that `/shared/graph_hashes.env` exists in the volume and contains the three lines.

```bash
docker compose down
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "Wire graph_hashes_shared into datacentre-api entrypoint"
```

---

### Task 13: Update `.env.example`

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Replace the file**

Open `.env.example` and replace its entire contents with:

```bash
# The JSON-RPC endpoint URL for the Polygon (or other EVM) network.
# This is used both by nginx (for /chain) and by graph-node for indexing.
BLOCKCHAIN_RPC_URL=""

# Authorization header value sent to the blockchain RPC endpoint.
BLOCKCHAIN_AUTH_HEADER=""

# The contract address for the XayaAccounts contract to be used for
# tracking moves.  The default value is the official contract
# on Polygon mainnet.
ACCOUNTS_CONTRACT="0x8C12253F71091b9582908C8a44F78870Ec6F304F"

# The Docker image to run for the GSP.  If custom options or variables
# are required for the GSP, this must already be contained in this container
# via an entrypoint.  Only standard arguments (such as Xaya RPC URL) will
# be passed to it.
GSP_IMAGE=""

# Game ID to use for activity and user tracking (sv for production, svt for test).
GAME_ID="sv"

# Path to the pre-synced GSP SQLite database file.
STORAGE_SQLITE_PATH="./storage.sqlite"

# Path to the archival MySQL dump to import on first start.
ARCHIVAL_SQL_PATH="./archival.sql"

# Path to the userconfig MySQL dump to import on first start.
USERCONFIG_SQL_PATH="./userconfig.sql"

# Host port for MariaDB external access (default: 3307 to avoid conflicts).
MARIADB_PORT="3307"
```

The two old `DATACENTRE_*_IMAGE` variables are removed — those services are now built from local sources.

- [ ] **Step 2: Verify**

```bash
diff .env.example .env 2>/dev/null || echo "Operator must update .env"
docker compose config --quiet
```

Expected: `docker compose config --quiet` exits 0 (compose file is valid).

- [ ] **Step 3: Commit**

```bash
git add .env.example
git commit -m "Drop image vars, add GAME_ID to .env.example"
```

---

### Task 14: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Rewrite the Architecture, Configuration, and Operational notes sections**

Open `CLAUDE.md`. Replace the existing `## Architecture` section (everything between `## Architecture` and `## Configuration`) with:

```markdown
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
- **`subgraph-deploy`** — one-shot. For each of `./subgraphs/{stats,sv,democrit}`: copies to writable scratch, runs `npm install` + `graph codegen` + `graph build` + `graph create` + `graph deploy`, then queries `graph-postgres` for the resulting IPFS hashes and writes them to `/shared/graph_hashes.env` in the `graph_hashes_shared` volume.
- **`datacentre-updater`** — built from `./datacentre_updater`. Reads SQLite + MariaDB; writes MariaDB (`datacentre`, `bigquery_local`) and the shared `datadumps` volume. The collation patch (`utf8mb4_0900_bin` → `utf8mb4_bin`) is applied at build time inside the vendored `Dockerfile`.
- **`datacentre-api`** — built from `./datacentre_api`. FastAPI behind Gunicorn. Entrypoint sources `/shared/graph_hashes.env` (auto-exported) before starting Python so `lookup_subgraph_schemas()` resolves locally-deployed subgraph hashes against `graph-postgres`'s `public.deployment_schemas`.
- **`healthcheck_xayax`** — sidecar, checks `xayax:8000` reachable.

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

### Vendored sources

```
forked-evm-testing/
├── datacentre_api/        snapshot of upstream FastAPI service
├── datacentre_updater/    snapshot of upstream updater (with build-time collation patch)
├── subgraphs/
│   ├── stats/             xaya-stats subgraph
│   ├── sv/                sv-subgraph
│   └── democrit/          democrit-sv subgraph
└── subgraph-deploy/       Dockerfile + deploy.sh for the one-shot deployer
```

Re-syncing from upstream (`C:\WorkFiles\datacentre_api`, `C:\WorkFiles\stats-subgraph`, etc.) is a manual `cp -r`. The collation patch in `datacentre_updater/docker/Dockerfile` is no-op-safe if upstream eventually fixes the source.

### Internal networking

Services reference each other by Docker hostname (`http://nginx/chain`, `http://graph-node:8000/subgraphs/name/sv`, `mysql://...@mariadb:3306/...`, `redis://redis:6379`, `graph-postgres:5432`, `ipfs:5001`). Only `nginx` and (optionally) `mariadb` (host port `${MARIADB_PORT}`, default 3307) are reachable from the host.

### Boot timing

Cold boot from `docker compose up -d --build`:
- ~30s — graph-node ready
- ~1-2m — `subgraph-deploy` finishes and writes `graph_hashes.env`
- ~2-30m — `mariadb` healthy (first-boot import depends on dump size)
- ~2m+ after the above — `datacentre-api` starts; MariaDB+SQLite endpoints work fully
- hours-days — graph-node indexing catches up; subgraph-backed endpoints (`/leaderboards`, `/user_activity`, `/shop`, parts of `/market`) progressively populate

Indexing volume can be substantial — graph-node will issue many `eth_getLogs`/`eth_call` requests to `BLOCKCHAIN_RPC_URL` during cold sync.

```

Then replace `## Configuration` with:

```markdown
## Configuration

Copy `.env.example` to `.env`. Required:

- `BLOCKCHAIN_RPC_URL` — full RPC URL (used by both nginx and graph-node)
- `BLOCKCHAIN_AUTH_HEADER` — `Authorization` header value (empty if endpoint needs none)
- `ACCOUNTS_CONTRACT` — XayaAccounts address
- `GSP_IMAGE` — Soccerverse GSP image

Optional:

- `GAME_ID` (default `sv`)
- `STORAGE_SQLITE_PATH`, `ARCHIVAL_SQL_PATH`, `USERCONFIG_SQL_PATH` — first-boot seed inputs
- `MARIADB_PORT` (default `3307`) — host-side port

All other configuration (Redis URL, Graph Postgres credentials, BigQuery local DB, dump folder paths, subgraph URLs, etc.) is hardcoded in `docker-compose.yml` because those are stack-internal Docker hostnames.
```

Then replace `## Operational notes` with:

```markdown
## Operational notes

- **First start is slow.** `mariadb` archival import + graph-node indexing both take time. Healthcheck `start_period` for mariadb is 30 minutes; for graph-node it's just service-up (indexing happens in the background).
- **`gsp-init` schema migrations are idempotent** (`2>/dev/null || true`). Add new ALTERs in the same pattern.
- **`subgraph-deploy` is idempotent.** Re-running on unchanged sources is fast — graph-cli detects unchanged manifests. The deploy script overwrites `/shared/graph_hashes.env` each run.
- **Collation patch in `datacentre_updater/docker/Dockerfile`** is no-op-safe — if upstream fixes `db_manager.py`, the sed simply matches nothing.
- **Editing `proxy.conf.template`:** the file is rendered through `envsubst` with the explicit allowlist `$BLOCKCHAIN_RPC_URL $BLOCKCHAIN_AUTH_HEADER` in `nginx/entrypoint.sh`. Any new env var referenced in the template must be added to that allowlist.
- **Stack memory footprint:** ~6-8 GB RAM during indexing. Postgres + graph-node + mariadb dominate.
- **Legacy directories**: `basechain/` and `helper/` remain in the tree from the original Anvil-fork design but are no longer wired into compose. Treat as inert unless re-introduced.
```

- [ ] **Step 2: Verify markdown is well-formed**

```bash
head -50 CLAUDE.md
wc -l CLAUDE.md
```

Spot-check that the headings are intact and there's no leftover content from the old version.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "Update CLAUDE.md for vendored datacentre + Graph indexing stack"
```

---

### Task 15: End-to-end smoke test

**Files:** none (validation only).

This is the integration checkpoint. It exercises the full cold-boot flow once.

- [ ] **Step 1: Ensure `.env` is populated**

Confirm `.env` has real values for `BLOCKCHAIN_RPC_URL`, `BLOCKCHAIN_AUTH_HEADER`, `GSP_IMAGE`. If not, fail this task with a note to the user — the smoke test cannot proceed without those.

- [ ] **Step 2: Cold-boot from a clean slate** (optional — only if testing first-boot behaviour)

If the operator wants a true cold boot:

```bash
docker compose down --volumes
```

WARNING: this destroys the existing `mariadb_data`, `graph_postgres_data`, `ipfs_data`, `gspdata`, etc. Only do this if you're prepared for the 30-minute MariaDB import.

For incremental testing, just:

```bash
docker compose down
```

- [ ] **Step 3: Bring everything up**

```bash
docker compose up -d --build
```

- [ ] **Step 4: Wait for `subgraph-deploy` to complete**

```bash
docker compose wait subgraph-deploy
docker compose logs subgraph-deploy | tail -20
```

Expected: exit code `0` from `wait`; tail shows three `Recorded GRAPH_SUBGRAPH_*=Qm...` lines.

If it fails, common causes:
- `graph-node` not yet healthy — retry the wait.
- `npm install` ECONNRESET — retry the run (transient).
- `subgraphs/<name>/package.json` requires newer graph-cli — bump the pin in `subgraph-deploy/Dockerfile`.

- [ ] **Step 5: Wait for the API to be healthy and verify schema lookup**

```bash
# Wait until the API container is up
until docker compose ps datacentre-api --format '{{.Status}}' | grep -q "Up"; do
  sleep 5
  echo "waiting for datacentre-api..."
done

docker compose logs datacentre-api 2>&1 | grep -E "Resolved IPFS hash|Could not find schema|features will be disabled"
```

Expected: two `Resolved IPFS hash QmXXX to schema sgdN` lines (one for stats, one for sv). NO `Could not find schema` lines.

- [ ] **Step 6: Hit immediately-working endpoints (MariaDB-backed)**

```bash
curl -s http://localhost:8100/api/clubs?per_page=2 | head -c 500; echo
curl -s http://localhost:8100/api/players?per_page=2 | head -c 500; echo
curl -s http://localhost:8100/api/users?per_page=2 | head -c 500; echo
curl -s http://localhost:8100/api/share_balances?per_page=2 | head -c 500; echo
curl -s http://localhost:8100/api/share_trade_history?per_page=2 | head -c 500; echo
```

Expected: each returns a valid JSON object/array. No 5xx errors.

- [ ] **Step 7: Hit subgraph-backed endpoints (may be empty until indexing catches up)**

```bash
curl -s "http://localhost:8100/api/leaderboards/clubs?per_page=2" | head -c 500; echo
curl -s "http://localhost:8100/api/user_activity?names=domob" | head -c 500; echo
curl -s "http://localhost:8100/api/shop/clubs?per_page=2" | head -c 500; echo
```

Expected: 200 status. Bodies may be empty arrays/objects until graph-node has indexed enough blocks. Watch progress with:

```bash
docker compose logs -f graph-node | grep -E "block_number|Sync"
```

- [ ] **Step 8: Confirm datacentre-updater is producing output**

```bash
docker compose logs datacentre-updater | tail -30
docker compose exec datacentre-api ls /dumps/
```

Expected: updater logs show no fatal errors; `/dumps/` may contain CSV/JSON files once the updater has run a full cycle.

- [ ] **Step 9: Tear down (optional)**

```bash
docker compose down
```

- [ ] **Step 10: Commit any final adjustments**

If Step 4-7 surfaced issues that required edits, commit them now with a descriptive message. If not, no commit needed — the previous tasks already cover the work.

---

## Self-Review Notes

The author of this plan should verify before handoff:

1. **Spec coverage** — every section of `2026-04-30-datacentre-stack-design.md` maps to a task:
   - Source layout → Task 1
   - mariadb extension → Task 3
   - datacentre-updater changes → Task 2 (Dockerfile patch) + Task 4 (compose changes)
   - datacentre-api changes → Task 5 (env) + Task 12 (entrypoint + shared volume)
   - Redis → Task 6
   - graph-postgres → Task 7
   - ipfs → Task 8
   - graph-node → Task 9
   - subgraph-deploy → Task 10 (build context) + Task 11 (compose service)
   - Volumes (`datadumps`, `redis_data`, `graph_postgres_data`, `ipfs_data`, `graph_hashes_shared`) → introduced incrementally in Tasks 4, 6, 7, 8, 11
   - .env.example → Task 13
   - CLAUDE.md → Task 14
   - Verification checklist → Task 15

2. **Type / name consistency** — service names (`datacentre-api`, `datacentre-updater`, `graph-postgres`, `graph-node`, `subgraph-deploy`), volume names, and env var names are spelled identically across all tasks. Hyphenated service names match Docker DNS rules.

3. **No placeholders** — every step contains either an exact command, exact code, or exact verification expectation. No "implement appropriately" or "etc."

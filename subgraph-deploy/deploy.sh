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

  # Skip if any ABI under abis/ contains the placeholder marker. This lets the
  # democrit subgraph ship with placeholder ABIs (see subgraphs/democrit/POPULATE_ABIS.md)
  # without breaking the rest of the stack. Operator populates real ABIs and
  # re-deploys to bring the subgraph online.
  if [ -d "${src}/abis" ] && grep -l _DATACENTRE_STACK_PLACEHOLDER "${src}/abis/"*.json >/dev/null 2>&1; then
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

  # Idempotent — if already created, graph-cli prints a warning and exits non-zero.
  npx graph create --node "${GRAPH_ADMIN_URL}" "${name}" || true

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

# Enabling the democrit subgraph

The democrit subgraph ships disabled by default. To enable it, the deploy host must supply **four** pieces of operator-specific configuration via env vars; otherwise `subgraph-deploy` skips democrit gracefully and the rest of the stack boots normally.

| Env var | What it is | When unset |
|---|---|---|
| `DEMOCRIT_ABI_PATH` | Host path to `Democrit.json` from `forge build` | Placeholder ABI used → democrit skipped |
| `VAULTMANAGER_ABI_PATH` | Host path to `VaultManager.json` from `forge build` | Placeholder ABI used → democrit skipped |
| `DEMOCRIT_CONTRACT_ADDRESS` | Deployed Democrit contract address on the chain | democrit skipped (envsubst would produce empty address) |
| `VAULTMANAGER_CONTRACT_ADDRESS` | Deployed VaultManager contract address on the chain | democrit skipped |
| `DEMOCRIT_START_BLOCK` | Indexer start block (defaults to `0`) | Indexes from block 0 — slow but correct |
| `DEMOCRIT_NETWORK` | Network name (defaults to `matic`) | Defaults applied |

All four required vars (`*_ABI_PATH`, `*_CONTRACT_ADDRESS`) must be set together. If any is missing, democrit is skipped — the trade-history endpoints in `datacentre-api` (those backed by `SVC_POLYGON_SUBGRAPH_URL`) will be unavailable, but everything else works.

## One-time setup on the deploy host

### 1. Install Foundry (if not already installed)

```bash
curl -L https://foundry.paradigm.xyz | bash
# open a new shell
foundryup
```

(See https://book.getfoundry.sh/getting-started/installation for other platforms.)

### 2. Build the contracts

The Solidity sources live in the `democrit-evm` repo (separate from this stack):

```bash
cd /path/to/democrit-evm
forge build
```

Produces `out/Democrit.sol/Democrit.json` and `out/VaultManager.sol/VaultManager.json` among other artefacts.

### 3. Find the deployed contract addresses

You need the addresses of the **already-deployed** Democrit and VaultManager contracts on the chain you're indexing (matches `DEMOCRIT_NETWORK` — defaults to `matic` for Polygon mainnet). Get them from your team's deployment records, an explorer, or the deployment scripts in `democrit-evm`. Also note the block at which Democrit was deployed — that's `DEMOCRIT_START_BLOCK` (using a much earlier value just wastes indexer time).

### 4. Set the env vars in `.env`

```bash
DEMOCRIT_ABI_PATH=/path/to/democrit-evm/out/Democrit.sol/Democrit.json
VAULTMANAGER_ABI_PATH=/path/to/democrit-evm/out/VaultManager.sol/VaultManager.json

DEMOCRIT_NETWORK=matic
DEMOCRIT_CONTRACT_ADDRESS=0x...
VAULTMANAGER_CONTRACT_ADDRESS=0x...
DEMOCRIT_START_BLOCK=12345678
```

Paths must be absolute. `DEMOCRIT_NETWORK` must match graph-node's chain config in `docker-compose.yml` (currently hardcoded as `matic`).

### 5. Re-deploy

`docker compose up -d`. No image rebuild required for env-var changes — `deploy.sh` copies the operator ABIs from `/abis-override/` over the baked-in placeholders, then runs `envsubst` on `subgraph.yaml` to inject the addresses + network + start block.

## Verifying

After `subgraph-deploy` runs:

```bash
docker compose logs subgraph-deploy | grep democrit
```

- `WARNING: democrit has placeholder ABIs — skipping deploy.` → at least one of `*_ABI_PATH` is unset or the file at that path still has the placeholder marker.
- `WARNING: democrit DEMOCRIT_CONTRACT_ADDRESS or VAULTMANAGER_CONTRACT_ADDRESS not set — skipping deploy.` → at least one of the address vars is missing.
- `Recorded GRAPH_SUBGRAPH_DEMOCRIT=Qm...` → democrit is live and indexing.

## Why this layout

`subgraphs/democrit/subgraph.yaml` upstream is a **template**, not a deployable subgraph: the network name and contract addresses are dummy testnet values, and the project is "not meant to be directly deployed, but used as part of a project using Democrit." Each downstream consumer fills in their own values.

This stack treats those values as deploy-time config. The subgraph source is baked into `${SUBGRAPH_DEPLOY_IMAGE}` with `${VAR}` placeholders in `subgraph.yaml`. At deploy time, `deploy.sh` runs `envsubst` against an allowlisted set of vars (`DEMOCRIT_NETWORK`, `DEMOCRIT_CONTRACT_ADDRESS`, `VAULTMANAGER_CONTRACT_ADDRESS`, `DEMOCRIT_START_BLOCK`) to produce a deployable `subgraph.yaml` for this specific environment. Operator ABIs are layered in via bind mounts to `/abis-override/`. No commit to this repo is needed when chain config or ABIs change — just update `.env` and redeploy.

# Populating democrit ABIs

The two ABI files in `abis/` (`Democrit.json`, `VaultManager.json`) are placeholders. They contain the marker key `_DATACENTRE_STACK_PLACEHOLDER` so the `subgraph-deploy` container can detect them and skip democrit gracefully (the stack still deploys; the democrit subgraph just isn't indexed).

To bring democrit online, supply real ABIs at runtime via two env vars — same pattern as `ARCHIVAL_SQL_PATH` / `USERCONFIG_SQL_PATH` / `STORAGE_SQLITE_PATH`. The compose file bind-mounts whatever paths you provide on top of the in-repo placeholders.

## One-time setup

1. **Install Foundry** (if not already installed)

   In Git Bash on the deploy PC:

   ```bash
   curl -L https://foundry.paradigm.xyz | bash
   ```

   Open a new shell, then:

   ```bash
   foundryup
   ```

   See https://book.getfoundry.sh/getting-started/installation for other platforms.

2. **Build the contracts**

   The Solidity sources live in the `democrit-evm` repo (separate from this stack). Locate it on the deploy PC, then:

   ```bash
   cd /path/to/democrit-evm
   forge build
   ```

   This produces `out/Democrit.sol/Democrit.json` and `out/VaultManager.sol/VaultManager.json` among other artefacts.

3. **Set the env vars in `.env`**

   ```bash
   DEMOCRIT_ABI_PATH=/path/to/democrit-evm/out/Democrit.sol/Democrit.json
   VAULTMANAGER_ABI_PATH=/path/to/democrit-evm/out/VaultManager.sol/VaultManager.json
   ```

   Paths are absolute (or relative to the docker-compose.yml directory). Both env vars must be set; if only one is, the other stays as a placeholder, deploy.sh detects the marker, and democrit is skipped.

4. **Re-deploy** — `subgraph-deploy` picks up the new ABIs on the next `docker compose up`. No image rebuild required, no commit to this repo required. The placeholder files in `subgraphs/democrit/abis/` stay untouched.

## Verifying the override worked

After `docker compose up -d` boots far enough for `subgraph-deploy` to run:

```bash
docker compose logs subgraph-deploy | grep democrit
```

If you see `WARNING: democrit has placeholder ABIs — skipping deploy.`, the env vars aren't taking effect — check that the paths exist on the host and the JSON files don't contain `_DATACENTRE_STACK_PLACEHOLDER`.

If you see `Recorded GRAPH_SUBGRAPH_DEMOCRIT=Qm...`, democrit is live.

## Why this layout?

The democrit subgraph at `subgraphs/democrit/subgraph.yaml` references its ABIs via relative paths (`./abis/Democrit.json`). Upstream, those paths were symlinks into a sibling Foundry project's `out/` directory, which only existed if `forge build` had been run. Vendoring symlinks across machines is fragile.

Instead, the stack ships placeholder ABIs in this folder so `git clone` always produces a deployable tree. The compose file then layers operator-supplied real ABIs on top via two env-var-driven bind mounts. The pattern matches how other generated artefacts (`archival.sql`, `userconfig.sql`, `storage.sqlite`) are supplied at deploy time. No commit to this repo is needed when ABIs change — just regenerate them and update the paths.

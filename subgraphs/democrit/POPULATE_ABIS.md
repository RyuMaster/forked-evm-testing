# Populating democrit ABIs

The two ABI files in `abis/` (`Democrit.json`, `VaultManager.json`) are placeholders. They contain the marker key `_DATACENTRE_STACK_PLACEHOLDER` so the `subgraph-deploy` container can detect them and skip democrit gracefully (the stack still deploys; the democrit subgraph just isn't indexed).

To bring democrit online, generate the real ABIs once:

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

   This produces `out/Democrit.sol/Democrit.json` and `out/VaultManager.sol/VaultManager.json`, among other artefacts.

3. **Replace the placeholders**

   From the root of this stack repo:

   ```bash
   cp /path/to/democrit-evm/out/Democrit.sol/Democrit.json    subgraphs/democrit/abis/Democrit.json
   cp /path/to/democrit-evm/out/VaultManager.sol/VaultManager.json subgraphs/democrit/abis/VaultManager.json
   ```

4. **Commit**

   ```bash
   git add subgraphs/democrit/abis/Democrit.json subgraphs/democrit/abis/VaultManager.json
   git commit -m "Populate democrit ABIs from forge build"
   ```

5. **Re-deploy** — `subgraph-deploy` will pick up the new ABIs on the next `docker compose up`. No image rebuild required.

## Verifying the placeholders are gone

```bash
grep -L _DATACENTRE_STACK_PLACEHOLDER subgraphs/democrit/abis/*.json
```

Both files should be listed (i.e. the marker is absent from both). If either still contains the marker, the deploy script will skip democrit.

## Why this layout?

The democrit subgraph at `subgraphs/democrit/subgraph.yaml` references its ABIs via relative paths (`./abis/Democrit.json`). Upstream, those paths were symlinks into a sibling Foundry project's `out/` directory, which only existed if `forge build` had been run. Vendoring symlinks across machines is fragile, so the stack instead inlines the ABI files directly. This keeps the vendored snapshot hermetic — once populated, no external repo or build step is needed at deploy time.

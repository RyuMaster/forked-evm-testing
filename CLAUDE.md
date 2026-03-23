# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Docker Compose-based testing infrastructure for Xaya blockchain gaming applications. Provides a forked EVM chain (Polygon by default via Foundry's Anvil), a connected Xaya X instance, a Game State Provider (GSP), and a Python JSON-RPC helper server — all behind an Nginx reverse proxy on `localhost:8100`.

## Common Commands

```bash
# Start the full environment (requires .env with BLOCKCHAIN_ENDPOINT and GSP_IMAGE set)
docker compose up --build

# Start in background
docker compose up --build -d

# Tear down
docker compose down

# Rebuild a specific service
docker compose build helper

# Run the integration test (requires running environment + Polygon mainnet fork)
ACCOUNTS_CONTRACT=0x8C12253F71091b9582908C8a44F78870Ec6F304F python test/helper-server.py
```

There is no linter, type checker, or automated test runner — the test is a single Python script (`test/helper-server.py`) run manually against a live Docker Compose environment.

## Architecture

**Service dependency chain:** basechain → healthcheck_chain → xayax → healthcheck_xayax → gsp. The helper service only depends on healthcheck_chain.

**Exposed endpoints (all via Nginx on port 8100):**
- `/chain` → Anvil RPC (port 8545), supports JSON-RPC and WebSocket
- `/gsp` → Game State Provider (port 8600)
- `/helper` → Python helper server (port 8000)

**Internal networking:** Services reference each other by Docker hostname through Nginx (e.g., `http://nginx/chain`), not localhost. Only Nginx is port-mapped to the host.

### Key Components

- **`basechain/`** — Anvil fork with `--auto-impersonate` and 5-second block time. Fork URL and block number are configured via env vars.
- **`helper/rpcserver.py`** — The main development surface. Python JSON-RPC server (jsonrpclib-pelix + web3.py) providing utility methods for testing: mining blocks, setting balances, transferring ERC-20 tokens, registering/transferring Xaya names, sending moves, admin/god-mode commands, and GSP sync.
- **`healthcheck/`** — Python scripts that verify basechain and Xaya X readiness via JSON-RPC calls. Used as Docker health checks to gate service startup.
- **`nginx/proxy.conf`** — Reverse proxy with lazy upstream resolution (services can start in any order). Sets `Content-Type: application/json` and supports WebSocket upgrade on `/chain`.
- **`test/helper-server.py`** — Integration test script asserting helper server functionality against a Polygon mainnet fork. Uses randomly generated addresses and assumes `p/domob` name exists.

## Code Patterns

- **Account impersonation:** Anvil's auto-impersonate is enabled globally. The helper server freely transacts as any address without private keys.
- **RPC method return convention:** Complex helper methods (`getname`, `sendmove`, `sendadmin`, `syncgsp`) return dicts with `success: True/False`, an `error` field on failure, and operation-specific fields.
- **ABI loading:** ABIs are loaded from JSON files in `test/abi/` (for tests) and copied into the helper container at `/abi/` (for the server). The JSON files have an `"abi"` key containing the ABI array.
- **WCHI token:** Resolved at startup from the XayaAccounts contract (`wchiToken()` call). Required for name registration; the `sendadmin` method has a multi-approach fallback to obtain WCHI (anvil_deal → contract transfer → accounts transfer).
- **Xaya name system:** Names have namespaces (`p/` for player, `g/` for game). Moves are sent via the `XayaAccounts.move()` function. Admin commands use the `g/tn` name.

## Configuration

Copy `.env.example` to `.env` and fill in:
- `BLOCKCHAIN_ENDPOINT` — Archival node RPC URL (e.g., Alchemy/Infura for Polygon)
- `FORK_BLOCK_NUMBER` — `"latest"` or a specific block height
- `ACCOUNTS_CONTRACT` — XayaAccounts address (default: Polygon mainnet)
- `GSP_IMAGE` — Docker image for the game's GSP

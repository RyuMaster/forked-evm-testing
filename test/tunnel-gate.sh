#!/usr/bin/env bash
# Tunnel gate check (native spec 2026-10-01-play-internal-test §3.2). Talks to ONE stack's nginx the way
# a phone does through the Cloudflare tunnel, and to the same nginx the way the LAN does.
#
#   TAURION_APP_KEY=... test/tunnel-gate.sh http://localhost:8101 test simulate
#   TAURION_APP_KEY=... test/tunnel-gate.sh https://anvil.taurion.io test public
#
# simulate: add a fake CF-Connecting-IP (what Cloudflare adds), so nginx treats the call as tunnel traffic.
# public:   Cloudflare adds it; the LAN checks are skipped (there is no LAN path through Cloudflare).
# Only harmless methods are used for the "refused" checks: if the gate is broken they must do no damage.
# The key is never echoed.
set -uo pipefail
BASE="${1:?base url}"; KIND="${2:?test|live}"; MODE="${3:?simulate|public}"
KEY="${TAURION_APP_KEY:?TAURION_APP_KEY must be set}"
fails=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; fails=$((fails + 1)); }
cf=(); [ "$MODE" = simulate ] && cf=(-H "CF-Connecting-IP: 203.0.113.7")

# status <path> <body> [extra curl args...] -> HTTP status of a tunnel-side JSON-RPC POST
status() { local p="$1" b="$2"; shift 2
  curl -s -o /dev/null -w '%{http_code}' -m 20 "${cf[@]}" "$@" -H 'Content-Type: application/json' \
       -d "$b" "$BASE$p"; }
# rpc <path> <body> -> response body of a keyed tunnel-side JSON-RPC POST
rpc() { curl -s -m 60 "${cf[@]}" -H "X-Taurion-Key: $KEY" -H 'Content-Type: application/json' -d "$2" "$BASE$1"; }
has_result() { python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if "result" in d else 1)' 2>/dev/null; }
err_code() { python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("error",{}).get("code",""))' 2>/dev/null; }

NULL='{"jsonrpc":"2.0","id":1,"method":"getnullstate","params":[]}'
# 1. no key / empty key / wrong key -> 403 on every route
for p in /gsp /chain; do
  [ "$(status $p "$NULL")" = 403 ] && pass "no key $p -> 403" || fail "no key $p -> 403"
  [ "$(status $p "$NULL" -H 'X-Taurion-Key;')" = 403 ] && pass "empty key $p -> 403" || fail "empty key $p -> 403"
  [ "$(status $p "$NULL" -H 'X-Taurion-Key: wrong')" = 403 ] && pass "wrong key $p -> 403" || fail "wrong key $p -> 403"
done
fs=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "${cf[@]}" "$BASE/feed/stream?v=2")
[ "$fs" = 403 ] && pass "no key /feed -> 403" || fail "no key /feed -> 403 (got $fs)"

# 2. keyed, allowlisted -> answered
rpc /gsp "$NULL" | has_result && pass "getnullstate answered" || fail "getnullstate answered"
cid=$(rpc /chain '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' | python3 -c 'import json,sys; print(json.load(sys.stdin).get("result",""))' 2>/dev/null)
[ "$cid" = 0x89 ] && pass "eth_chainId 0x89" || fail "eth_chainId 0x89 (got $cid)"

# 3. keyed, NOT allowlisted -> JSON-RPC -32601, never forwarded (harmless methods only)
[ "$(rpc /gsp '{"jsonrpc":"2.0","id":2,"method":"getpendingstate","params":[]}' | err_code)" = -32601 ] \
  && pass "gsp getpendingstate refused" || fail "gsp getpendingstate refused"
[ "$(rpc /chain '{"jsonrpc":"2.0","id":3,"method":"eth_accounts","params":[]}' | err_code)" = -32601 ] \
  && pass "chain eth_accounts refused" || fail "chain eth_accounts refused"
[ "$(rpc /chain '[{"jsonrpc":"2.0","id":4,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":5,"method":"eth_accounts","params":[]}]' | err_code)" = -32601 ] \
  && pass "batch with one refused method refused whole" || fail "batch with one refused method refused whole"
[ "$(rpc /gsp 'not json' | err_code)" = -32700 ] && pass "garbage -> -32700" || fail "garbage -> -32700"

# 4. large answers arrive whole through the gate (getbuildings is ~330 KB: far past nginx's default
#    one-page subrequest buffer, which getbootstrapdata at ~1.3 KB never exercised)
rpc /gsp '{"jsonrpc":"2.0","id":6,"method":"getbootstrapdata","params":[]}' | has_result \
  && pass "getbootstrapdata whole" || fail "getbootstrapdata whole"
rpc /gsp '{"jsonrpc":"2.0","id":9,"method":"getbuildings","params":[]}' | has_result \
  && pass "getbuildings (~330 KB) whole" || fail "getbuildings (~330 KB) whole"

# 5. helper (anvil only): an arity error is the "alive" answer the app's probe expects
if [ "$KIND" = test ]; then
  c=$(rpc /helper '{"jsonrpc":"2.0","id":7,"method":"getname","params":[]}' | err_code)
  [ -n "$c" ] && [ "$c" -lt 0 ] 2>/dev/null && pass "helper getname answers (code $c)" || fail "helper getname answers"
  [ "$(rpc /helper '{"jsonrpc":"2.0","id":8,"method":"nosuchmethod","params":[]}' | err_code)" = -32601 ] \
    && pass "helper unknown method refused" || fail "helper unknown method refused"
fi

# 6. feed: keyed stream opens; 13 streams from 13 client IPs all open (the fanout caps 12 per IP)
ct=$(curl -s -o /dev/null -w '%{content_type}' -m 3 "${cf[@]}" -H "X-Taurion-Key: $KEY" "$BASE/feed/stream?v=2")
case "$ct" in text/event-stream*) pass "feed stream opens";; *) fail "feed stream opens (got '$ct')";; esac
if [ "$MODE" = simulate ]; then
  tmp=$(mktemp -d)
  for i in $(seq 1 13); do
    curl -s -o /dev/null -w '%{http_code}\n' -m 4 -H "CF-Connecting-IP: 198.51.100.$i" \
         -H "X-Taurion-Key: $KEY" "$BASE/feed/stream?v=2" > "$tmp/$i" &
  done; wait
  ok=$(cat "$tmp"/* | grep -c '^200$'); rm -rf "$tmp"
  [ "$ok" = 13 ] && pass "13 testers, 13 streams" || fail "13 testers, 13 streams (got $ok)"
fi

# 7. LAN behaviour unchanged (no CF header, no key)
if [ "$MODE" = simulate ]; then
  curl -s -m 20 -H 'Content-Type: application/json' -d "$NULL" "$BASE/gsp" | has_result \
    && pass "LAN getnullstate unchanged" || fail "LAN getnullstate unchanged"
fi

echo "$fails failure(s)"; exit $((fails > 0))

#!/usr/bin/env python3

"""
One-shot warm-up of anvil's forked-state cache, run to completion before the
helper starts serving.

WHY THIS EXISTS
---------------
anvil fetches forked account state LAZILY from the upstream RPC and then caches
it for the life of the chain (and, via --state, across restarts).  Every address
is therefore fetched from upstream exactly once -- the first time anything
touches it -- and a freshly created stack has an empty cache.

The upstream is a public, load-balanced pool, and it does not answer those first
fetches reliably.  Some backends reply to a cold account read with a well-formed
JSON-RPC error

    -32000  getStateObject (<addr>) error: account 0x<addr> is not found

instead of an empty account.  Measured against the live pool on 2026-08-19:
SEVEN OF EIGHT identical reads of the XayaPolicy account failed that way, and
the eighth returned 0x0 and cached it, after which the path was permanently
fine.

anvil's --retries / --fork-retry-backoff DO NOT COVER THIS, and raising them
will not help: they retry TRANSPORT failures, whereas a well-formed JSON-RPC
error object is an authoritative answer.  anvil accepts it and re-raises it as

    -32603  failed to get account for 0x...

so whatever asked first is what fails.  In practice that was a player tapping
"Create" on a freshly recreated stack: registration walks accounts -> policy ->
WCHI, and the policy contract's address surfaced on the phone inside "Failed to
register faction: helper 'getname' failed".  Nothing was actually wrong with the
stack -- it had simply never fetched that account before.

So the retrying belongs HERE, at boot, where it is correct, unattended, and
where a failure is a log line instead of a player-visible error.

THIS SCRIPT IS READ-ONLY.  eth_getBalance / eth_getCode / eth_call only.  It
never sends a transaction, never mines a block and never impersonates an
account, so it cannot change chain state no matter how often it runs.
"""

import json
import os
import sys
import time

from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.middleware import ExtraDataToPOAMiddleware


chainRpc = "http://nginx/chain"

# The app's emulated name receiver (ChainConstants.NAME_RECEIVER in the client).
# It is the "from" of every registration, so it sits on the cold path too, but
# nothing on the chain can tell us about it -- hence a default that a deployment
# can override via WARMUP_EXTRA_ADDRESSES.
DEFAULT_EXTRA_ADDRESSES = "0x1234567890123456789012345678901234567890"

# A name used only for the read-only registration dry run below.  It does not
# matter whether it exists: if it does, register() reverts, and a revert warms
# the state just as well as a success.
PROBE_NAME = "warmup-probe"

# How long to keep retrying cold fetches in total.  Generous, because the pool's
# bad spells last minutes and the cost of waiting here is invisible, while the
# cost of not waiting lands on a player.
BUDGET_SECONDS = float (os.getenv ("WARMUP_BUDGET_SECONDS", "180"))

RETRY_PAUSE_SECONDS = 1.0

# The upstream pool's ways of refusing a cold fetch, plus anvil's wrapper around
# them.  Anything NOT matching these means the EVM actually ran (or the request
# was malformed), and if it ran then the state it needed was fetched -- which is
# all this script is trying to achieve.
COLD_FETCH_MARKERS = (
  "failed to get account",
  "is not found",
  "historical state",
)


def log (msg):
  print ("warm-up: %s" % msg, flush=True)


def loadAbi (nm):
  with open (os.path.join ("/abi", "%s.json" % nm), "rt") as f:
    data = json.load (f)
  return data["abi"]


def isColdFetchFailure (exc):
  """Whether exc is the upstream refusing a cold state fetch (i.e. retryable)."""

  msg = str (exc).lower ()
  return any (m in msg for m in COLD_FETCH_MARKERS)


def retryAlways (exc):
  return True


def withRetry (what, fn, deadline, shouldRetry=isColdFetchFailure):
  """
  Calls fn until it succeeds or the deadline passes.  Returns (ok, value).

  Only failures shouldRetry accepts are retried.  Everything else is reported
  and abandoned at once: spending the whole budget on a revert or a
  misconfiguration would delay the stack without any chance of helping.
  """

  attempt = 0
  while True:
    attempt += 1
    try:
      value = fn ()
      if attempt > 1:
        log ("  %s: ok after %d attempts" % (what, attempt))
      return True, value
    except Exception as exc:
      if not shouldRetry (exc):
        log ("  %s: giving up, not a cold-fetch failure: %s" % (what, exc))
        return False, None
      if time.time () >= deadline:
        log ("  %s: STILL COLD after %d attempts: %s" % (what, attempt, exc))
        return False, None
      time.sleep (RETRY_PAUSE_SECONDS)


def warmAddress (label, addr, deadline):
  """
  Pulls an address' account into anvil's cache.  Returns True if it is warm.

  Balance and code are two separate lazy fetches, so both are asked for.
  """

  okBalance, _ = withRetry ("%s balance" % label,
                            lambda: w3.eth.get_balance (addr, "latest"), deadline)
  okCode, _ = withRetry ("%s code" % label,
                         lambda: w3.eth.get_code (addr, "latest"), deadline)
  return okBalance and okCode


w3 = Web3 (Web3.HTTPProvider (chainRpc))
w3.middleware_onion.inject (ExtraDataToPOAMiddleware, layer=0)


def main ():
  deadline = time.time () + BUDGET_SECONDS
  log ("starting, budget %.0fs, chain %s" % (BUDGET_SECONDS, chainRpc))

  # 1. The chain has to answer at all.
  #
  # healthcheck_chain only proves that nginx can reach anvil's eth_chainId, and
  # anvil serves that from memory the moment it binds its port -- before the
  # fork can serve any state.  That gap is a real one: on 2026-08-19 the helper
  # started inside it and died at import on "Excess blob gas not set", then came
  # back clean 30s later.  Asking for a block number here closes it, because
  # this service completing is what the helper now waits for.
  ok, _ = withRetry ("chain reachable", lambda: w3.eth.block_number, deadline,
                     shouldRetry=retryAlways)
  if not ok:
    log ("chain never became reachable; leaving the cache cold")
    return

  # 2. Resolve the contracts rather than hard-coding them, so this keeps warming
  #    the right addresses if the deployment's accounts contract ever changes.
  accountsAddr = Web3.to_checksum_address (os.getenv ("ACCOUNTS_CONTRACT"))
  accounts = w3.eth.contract (address=accountsAddr, abi=loadAbi ("IXayaAccounts"))

  targets = [("accounts", accountsAddr)]

  ok, policyAddr = withRetry ("resolve policy",
                              accounts.functions.policy ().call, deadline)
  if ok:
    targets.append (("policy", policyAddr))

  ok, wchiAddr = withRetry ("resolve WCHI",
                            accounts.functions.wchiToken ().call, deadline)
  if ok:
    targets.append (("WCHI", wchiAddr))

  extras = [a.strip () for a in
            os.getenv ("WARMUP_EXTRA_ADDRESSES", DEFAULT_EXTRA_ADDRESSES).split (",")
            if a.strip ()]
  for i, addr in enumerate (extras):
    targets.append (("extra[%d]" % i, Web3.to_checksum_address (addr)))

  cold = []
  for label, addr in targets:
    log ("%s %s" % (label, addr))
    if not warmAddress (label, addr, deadline):
      cold.append ("%s (%s)" % (label, addr))

  # 3. The strongest warm-up available: dry-run the real registration.
  #
  # Warming an account caches the ACCOUNT; it does not cache that account's
  # individual storage slots, which anvil also fetches lazily and one at a time.
  # An eth_call of register() walks the exact path a player's "Create" walks --
  # accounts storage, then the policy contract, then WCHI -- so the slots that
  # path needs get fetched here rather than there.  It writes nothing.
  #
  # A REVERT COUNTS AS SUCCESS.  A revert means the EVM executed, and it could
  # only execute after the state was fetched.  On a fresh chain the receiver
  # holds no WCHI yet, so reverting is in fact the expected outcome; treating it
  # as a failure would make the common case look broken.
  if extras:
    def dryRunRegister ():
      try:
        accounts.functions.register ("p", PROBE_NAME).call ({"from": extras[0]})
        return "would succeed"
      except ContractLogicError as exc:
        return "reverted, which warms the same state: %s" % exc

    ok, outcome = withRetry ("register dry run", dryRunRegister, deadline)
    if ok:
      log ("register dry run: %s" % outcome)
    else:
      cold.append ("register path storage")

  # 4. Report.
  if cold:
    log ("WARNING -- %d target(s) still cold: %s" % (len (cold), "; ".join (cold)))
    log ("the stack will still start, but the first action touching one of "
         "these may fail with \"failed to get account for 0x...\"; re-running "
         "this service once the upstream recovers is enough to clear it.")
  else:
    log ("all targets warm")


# ALWAYS EXIT 0.
#
# The helper gates on this service COMPLETING, and that ordering is the
# guarantee actually needed -- the helper must not import against a fork that
# cannot yet serve state.  Exiting non-zero would hold the helper, and with it
# the entire registration path, down for as long as the upstream pool is having
# a bad minute, which turns a degraded stack into a dead one.  A cold address
# only matters if something touches it, so the honest response to a failed
# warm-up is to say so loudly and let the stack come up.
#
# Guarded so the classification helpers above can be imported and tested without
# a chain: whether a given upstream message counts as "retry this" is the one
# decision here that a green run cannot demonstrate.
if __name__ == "__main__":
  main ()
  sys.exit (0)

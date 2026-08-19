#!/usr/bin/env python3

"""
Checks how the boot-time warm-up classifies upstream failures.

Unlike the other script in here this one needs NO running stack: it is pure
decision logic.  That is deliberate.  A warm-up run against a healthy chain
prints "all targets warm" and proves almost nothing -- the interesting behaviour
only appears when the upstream misbehaves, which cannot be summoned on demand.
The one thing that must be right is which failures are worth retrying:

  - retry a cold fetch, because a later attempt genuinely does succeed
    (measured 2026-08-19: seven of eight reads failed, the eighth cached the
    account and every read after it succeeded);
  - never retry a revert, because the EVM ran, which means the state was
    already fetched -- retrying would burn the whole budget to re-learn that.

Get that backwards and the warm-up either gives up while the fix was one more
attempt away, or holds the stack for three minutes over a contract that did
exactly what it should.

Run:  python3 test/warmup-classification.py
"""

import os
import sys

sys.path.insert (0, os.path.join (os.path.abspath (os.path.dirname (__file__)),
                                  "..", "helper"))

import warmup

from web3.exceptions import ContractLogicError


failures = []


def check (what, got, want):
  if got == want:
    print ("  ok    %s" % what)
  else:
    print ("  FAIL  %s: got %r, want %r" % (what, got, want))
    failures.append (what)


# The messages below are VERBATIM from the 2026-08-19 outage, not paraphrases.
# They are the contract this classification is written against, so a change in
# how the upstream words its refusal should break this file loudly.
ANVIL_WRAPPER = (
  "failed to get account for 0x997A8B19d200A453D77c3857E81Af31F680b3663: "
  "server returned an error response: error code -32000: "
  "getStateObject (997a8b19d200a453d77c3857e81af31f680b3663) error: "
  "account 0x997a8b19d200a453d77c3857e81af31f680b3663 is not found")

UPSTREAM_BARE = (
  "getStateObject (997a8b19d200a453d77c3857e81af31f680b3663) error: "
  "account 0x997a8b19d200a453d77c3857e81af31f680b3663 is not found")

# The fork block ageing out of the upstream's pruning window. Same class of
# problem -- the state we need is not being served -- and worth retrying for the
# same reason, since the pool is not uniform.
PRUNED_STATE = ("historical state at block 6b4ff0c5... is not available, "
                "pruning depth exceeded")

print ("cold-fetch failures (must be retried):")
check ("anvil's wrapper around the upstream error",
       warmup.isColdFetchFailure (Exception (ANVIL_WRAPPER)), True)
check ("the bare upstream -32000 body",
       warmup.isColdFetchFailure (Exception (UPSTREAM_BARE)), True)
check ("a dict-shaped Web3RPCError payload",
       warmup.isColdFetchFailure (
         Exception ({"code": -32603, "message": ANVIL_WRAPPER})), True)
check ("historical state pruned away",
       warmup.isColdFetchFailure (Exception (PRUNED_STATE)), True)

print ("everything else (must NOT be retried):")
check ("a plain revert -- the EVM ran, so the state is already warm",
       warmup.isColdFetchFailure (ContractLogicError ("execution reverted")), False)
check ("a named revert reason",
       warmup.isColdFetchFailure (
         ContractLogicError ("execution reverted: name already registered")), False)
check ("a bad argument",
       warmup.isColdFetchFailure (ValueError ("invalid address checksum")), False)

# The deliberately awkward one. "is not found" is a broad marker, so a revert
# whose reason happens to contain those words would be retried pointlessly for
# the whole budget. This records that the marker is known to be broad and that
# the cost of the mistake is bounded (a slow boot, never a wrong result); tighten
# COLD_FETCH_MARKERS if a real contract ever reverts with this wording.
print ("known imprecision (documented, not a bug):")
check ("a revert whose reason contains 'is not found' is retried",
       warmup.isColdFetchFailure (ContractLogicError ("account is not found")), True)

print ("withRetry control flow:")

warmup.RETRY_PAUSE_SECONDS = 0

attempts = {"n": 0}


def coldTwiceThenFine ():
  attempts["n"] += 1
  if attempts["n"] < 3:
    raise Exception (ANVIL_WRAPPER)
  return "warm"


import time

ok, value = warmup.withRetry ("cold twice", coldTwiceThenFine, time.time () + 30)
check ("retries a cold fetch until it succeeds", (ok, value), (True, "warm"))
check ("and stops as soon as it does", attempts["n"], 3)

nonRetryable = {"n": 0}


def alwaysReverts ():
  nonRetryable["n"] += 1
  raise ContractLogicError ("execution reverted")


ok, _ = warmup.withRetry ("revert", alwaysReverts, time.time () + 30)
check ("abandons a non-retryable failure", ok, False)
check ("without a second attempt", nonRetryable["n"], 1)

expired = {"n": 0}


def alwaysCold ():
  expired["n"] += 1
  raise Exception (ANVIL_WRAPPER)


# A deadline already in the past: the budget must be honoured even for a failure
# that is retryable, or a bad upstream spell would hold the stack indefinitely.
ok, _ = warmup.withRetry ("expired", alwaysCold, time.time () - 1)
check ("honours an exhausted budget", ok, False)
check ("after trying exactly once", expired["n"], 1)

print ()
if failures:
  sys.exit ("%d check(s) FAILED: %s" % (len (failures), "; ".join (failures)))
print ("all checks passed")

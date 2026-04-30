import {
  assert,
  afterEach,
  clearStore,
  describe,
  newMockEvent,
  test,
} from "matchstick-as/assembly/index"

import {
  BigInt,
  ethereum,
} from "@graphprotocol/graph-ts"

import {
  ReferrerUpdated as ReferrerUpdatedEvent,
} from "../generated/ReferralTracker/ReferralTracker"

import {
  ReferralBonusGiven as ReferralBonusGivenEvent,
} from "../generated/templates/PackSaleForReferrals/SwappingPackSale"

import {
  Referral,
  Referrer,
  ReferrerTotal,
} from "../generated/schema"

import {
  accountToBytes,
  handleReferralBonus,
  handleReferrerUpdated,
} from "../src/referralTracker"

/* ************************************************************************** */

/**
 * Helper function to create a ReferrerUpdated mock event and process it.
 */
function testReferrerUpdated (name: String, referrer: String,
                              timestamp: i32): void
{
  const ev = changetype<ReferrerUpdatedEvent> (newMockEvent ())
  ev.block.timestamp = BigInt.fromI32 (timestamp)
  ev.parameters = new Array ()
  ev.parameters.push (
      new ethereum.EventParam ("name", ethereum.Value.fromString (name)))
  ev.parameters.push (
      new ethereum.EventParam ("referrer",
                               ethereum.Value.fromString (referrer)))
  ev.parameters.push (
      new ethereum.EventParam ("timestamp", ethereum.Value.fromI32 (timestamp)))
  handleReferrerUpdated (ev)
}

/**
 * Helper function to create a ReferralBonus mock event and process it.
 * The referrer is derived from the store automatically.
 */
function testReferralBonus (buyer: String,
                            clubId: i32, bonusShares: i32,
                            packs: i32, usd: i32,
                            timestamp: i32): void
{
  const referral = Referral.load (accountToBytes (buyer))!
  const referrer = Referrer.load (referral.referrer)!.account

  const ev = changetype<ReferralBonusGivenEvent> (newMockEvent ())
  ev.block.timestamp = BigInt.fromI32 (timestamp)
  /* Fake some logIndex (which is used to construct the ID of the
     ReferrerBonus entity) that is unique by using the timestamp.  */
  ev.logIndex = BigInt.fromI32 (timestamp)

  ev.parameters = new Array ()
  ev.parameters.push (
      new ethereum.EventParam ("buyer", ethereum.Value.fromString (buyer)))
  ev.parameters.push (
      new ethereum.EventParam ("referrer",
                               ethereum.Value.fromString (referrer)))
  ev.parameters.push (
      new ethereum.EventParam ("clubId", ethereum.Value.fromI32 (clubId)))
  ev.parameters.push (
      new ethereum.EventParam ("numShares",
                               ethereum.Value.fromI32 (bonusShares)))
  ev.parameters.push (
      new ethereum.EventParam ("numPacksBought",
                               ethereum.Value.fromI32 (packs)))
  ev.parameters.push (
      new ethereum.EventParam ("cost", ethereum.Value.fromI32 (usd)))
  handleReferralBonus (ev)
}

/**
 * Helper function that checks that a given Referral exists in the store
 * with the given referrer and creation timestamp.
 */
function assertReferral (name: String, referrer: String, timestamp: i32): void
{
  const id = accountToBytes (name).toHexString ()
  const refId = accountToBytes (referrer).toHexString ()
  assert.fieldEquals ("Referral", id, "account", name)
  assert.fieldEquals ("Referral", id, "referrer", refId)
  assert.fieldEquals ("Referral", id, "timestamp", timestamp.toString ())
}

/**
 * Helper function to check that a given Referral does not exist.
 */
function assertNoReferral (name: String): void
{
  const id = accountToBytes (name).toHexString ()
  assert.notInStore ("Referral", id)
}

/**
 * Helper function to check that a given Referrer exists in the store
 * with the expected current totals.
 */
function assertReferrer (name: String, referrals: i32,
                         bonusShares: i32, usdSpent: i32): void
{
  const id = accountToBytes (name).toHexString ()
  assert.fieldEquals ("Referrer", id, "account", name)
  const ref = Referrer.load (accountToBytes (name))!

  const cur = ReferrerTotal.load (ref.currentTotal)!
  assert.i32Equals (cur.referrals.toI32 (), referrals)
  assert.i32Equals (cur.bonusShares.toI32 (), bonusShares)
  assert.i32Equals (cur.usdSpent.toI32 (), usdSpent)
}

/**
 * Helper function to assert that the totals recorded for a given referrer
 * at a given timestamp match the expected values.
 */
function assertTotals (name: String, timestamp: i32,
                       referrals: i32, bonusShares: i32, usdSpent: i32): void
{
  const ref = Referrer.load (accountToBytes (name))!
  const totals = ref.totals.load ()

  let found = false
  for (let i = 0; i < totals.length; ++i)
    {
      if (totals[i].timestamp.toI32 () != timestamp)
        continue

      assert.i32Equals (totals[i].referrals.toI32 (), referrals)
      assert.i32Equals (totals[i].bonusShares.toI32 (), bonusShares)
      assert.i32Equals (totals[i].usdSpent.toI32 (), usdSpent)
      found = true
    }

  assert.assertTrue (found)
}

/**
 * Helper function to check that a given ReferrerBonus entry exists.  The
 * referrer is looked up automatically based on the referral, and we look for
 * and compare an entry based on the timestamp.
 */
function assertBonus (buyer: String, timestamp: i32,
                      clubId: i32, packsBought: i32,
                      usdSpent: i32, bonusShares: i32): void
{
  const refId = accountToBytes (buyer)
  const referral = Referral.load (refId)!
  const referrer = Referrer.load (referral.referrer)!
  const bonuses = referrer.bonuses.load ()

  let found = false
  for (let i = 0; i < bonuses.length; ++i)
    {
      if (bonuses[i].timestamp.toI32 () != timestamp)
        continue

      assert.bytesEquals (bonuses[i].referral, refId)
      assert.i32Equals (bonuses[i].clubId.toI32 (), clubId)
      assert.i32Equals (bonuses[i].packsBought.toI32 (), packsBought)
      assert.i32Equals (bonuses[i].usdSpent.toI32 (), usdSpent)
      assert.i32Equals (bonuses[i].bonusShares.toI32 (), bonusShares)

      found = true
    }

  assert.assertTrue (found)
}

/* ************************************************************************** */

afterEach (clearStore)

describe ("ReferralTracker", () => {

  test ("Referral entities", () => {
    testReferrerUpdated ("domob", "referrer", 10)
    testReferrerUpdated ("andy", "referrer", 20)

    assertReferral ("domob", "referrer", 10)
    assertReferral ("andy", "referrer", 20)

    testReferrerUpdated ("domob", "", 30)
    testReferrerUpdated ("andy", "other referrer", 40)

    assertReferral ("andy", "other referrer", 40)
    assertNoReferral ("domob")
  })

  test ("Referrer and totals", () => {
    testReferrerUpdated ("domob", "referrer", 10)
    assertReferrer ("referrer", 1, 0, 0)

    testReferrerUpdated ("andy", "referrer", 20)
    testReferralBonus ("domob", 123, 20, 2, 500, 30)
    assertReferrer ("referrer", 2, 20, 500)

    testReferrerUpdated ("domob", "", 40)
    assertReferrer ("referrer", 1, 20, 500)

    testReferrerUpdated ("andy", "other referrer", 50)
    assertReferrer ("referrer", 0, 20, 500)
    assertReferrer ("other referrer", 1, 0, 0)

    assertTotals ("referrer", 10, 1, 0, 0)
    assertTotals ("referrer", 20, 2, 0, 0)
    assertTotals ("referrer", 30, 2, 20, 500)
    assertTotals ("referrer", 40, 1, 20, 500)
    assertTotals ("referrer", 50, 0, 20, 500)
  })

  test ("ReferrerBonus", () => {
    testReferrerUpdated ("domob", "referrer", 10)
    testReferrerUpdated ("andy", "referrer", 20)
    testReferralBonus ("domob", 123, 20, 2, 500, 30)
    testReferralBonus ("andy", 555, 15, 7, 999, 40)

    assertBonus ("domob", 30, 123, 2, 500, 20)
    assertBonus ("andy", 40, 555, 7, 999, 15)
  })

})

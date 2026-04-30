import {
  assert,
  afterEach,
  clearStore,
  describe,
  test,
} from "matchstick-as/assembly/index"

import {
  Address,
  Bytes,
} from "@graphprotocol/graph-ts"

import {
  Address as AddressEntity,
  Game as GameEntity,
  Name as NameEntity,
  Namespace as NamespaceEntity,
  Transaction as TransactionEntity,
} from "../generated/schema"

import {
  ALICE,
  BOB,
  TXID,
  testMove,
  testMoveWithPayment,
  testRegistration,
  testTransfer,
} from "./testutils"

import {
  tokenIdForName,
  tokenIdToBytes,
} from "../src/util"

/* ************************************************************************** */

/**
 * Helper function to assert that an Address exists in the database.
 */
function assertAddress (addr: Address): void
{
  assert.assertNotNull (AddressEntity.load (addr))
}

/**
 * Helper function to assert that a Transaction exists in the database.
 */
function assertTransaction (txHash: Bytes): void
{
  const txEntity = TransactionEntity.load (txHash)
  assert.assertNotNull (txEntity)
  assert.fieldEquals ("Transaction", txHash.toHexString (), "height", "1000")
  assert.fieldEquals ("Transaction", txHash.toHexString (), "timestamp", "100")
}

/**
 * Helper function that checks that a given Namespace entity exists.
 */
function assertNamespace (ns: String): void
{
  const id = Bytes.fromUTF8 (ns)
  assert.fieldEquals ("Namespace", id.toHexString (), "ns", ns)
}

/**
 * Helper function that checks that a given Game entity exists.
 */
function assertGame (game: String): void
{
  const id = Bytes.fromUTF8 (game)
  assert.fieldEquals ("Game", id.toHexString (), "game", game)
}

/**
 * Helper function to assert that a Name entry exists and has the given owner.
 * This also checks that it has an associated Registration entry that
 * is consistent.
 */
function assertName (ns: String, name: String, owner: Address): void
{
  const id = tokenIdToBytes (tokenIdForName (ns, name))
  const nameEntity = NameEntity.load (id)
  assert.assertNotNull (nameEntity)

  const nsEntity = NamespaceEntity.load (nameEntity!.ns)!

  assert.stringEquals (nsEntity.ns, ns)
  assert.stringEquals (nameEntity!.name, name)
  assert.bytesEquals (nameEntity!.owner, owner)

  assert.i32Equals (nameEntity!.registration.load ().length, 1)
}

/**
 * Helper function that checks that a given number of Move entries is
 * recorded for a name.
 */
function assertNumMoves (ns: String, name: String, num: i32): void
{
  const id = tokenIdToBytes (tokenIdForName (ns, name))
  const nameEntity = NameEntity.load (id)!

  assert.i32Equals (nameEntity.moves.load ().length, num)
}

/**
 * Helper function that checks that one of the moves for the given name
 * has a Payment associated with the given data.
 */
function assertMovePayment (ns: String, name: String,
                            amount: i32, receiver: Address): void
{
  const id = tokenIdToBytes (tokenIdForName (ns, name))
  const nameEntity = NameEntity.load (id)!

  let found = false
  const moves = nameEntity.moves.load ()
  for (let i = 0; i < moves.length; ++i)
    {
      const payments = moves[i].payment.load ()
      if (payments.length == 0)
        continue;

      assert.i32Equals (payments.length, 1)
      if (payments[0].amount.toI32 () == amount
            && payments[0].receiver == receiver)
        found = true
    }
  assert.assertTrue (found)
}

/* ************************************************************************** */

afterEach (clearStore)

describe ("Registration", () => {

  test ("creates Address correctly", () => {
    testRegistration ("p", "domob", ALICE)
    testRegistration ("p", "andy", ALICE)
    assertAddress (ALICE)
    assert.notInStore ("Address", BOB.toHexString ())
  })

  test ("creates Transaction reference correctly", () => {
    testRegistration ("p", "domob", ALICE)

    assertTransaction (TXID)

    const nameId = tokenIdToBytes (tokenIdForName ("p", "domob"))
    const registrations = NameEntity.load (nameId)!.registration.load ()
    assert.i32Equals (registrations.length, 1)
    assert.bytesEquals (registrations[0].tx, TXID)
  })

  test ("creates Namespace correctly", () => {
    testRegistration ("p", "domob", ALICE)
    assertNamespace ("p")
    assert.notInStore ("Namespace", Bytes.fromUTF8 ("g").toHexString ())

    testRegistration ("g", "domob", BOB)
    assertNamespace ("p")
    assertNamespace ("g")
  })

  test ("creates Name entries", () => {
    testRegistration ("p", "domob", ALICE)
    assertName ("p", "domob", ALICE)

    const id = tokenIdToBytes (tokenIdForName ("g", "domob"))
    assert.notInStore ("Name", id.toHexString ())

    testRegistration ("g", "domob", BOB)
    assertName ("p", "domob", ALICE)
    assertName ("g", "domob", BOB)
  })

})

describe ("Transfer", () => {

  test ("creates Address correctly", () => {
    testRegistration ("p", "domob", ALICE)
    testTransfer ("p", "domob", ALICE, BOB)
    assertAddress (BOB)
  })

  test ("updates name ownership", () => {
    testRegistration ("p", "domob", ALICE)
    testRegistration ("p", "andy", ALICE)
    assertName ("p", "domob", ALICE)
    assertName ("p", "andy", ALICE)

    testTransfer ("p", "domob", ALICE, BOB)
    assertName ("p", "domob", BOB)
    assertName ("p", "andy", ALICE)
  })

})

describe ("Moves", () => {

  test ("creates Move entries for names", () => {
    testRegistration ("p", "domob", ALICE)
    testRegistration ("p", "andy", ALICE)
    assertNumMoves ("p", "domob", 0)
    assertNumMoves ("p", "andy", 0)

    testMove ("p", "domob", "{}")
    assertNumMoves ("p", "domob", 1)
    assertNumMoves ("p", "andy", 0)

    testMove ("p", "andy", "{}")
    testMove ("p", "domob", "{}")
    assertNumMoves ("p", "domob", 2)
    assertNumMoves ("p", "andy", 1)
  })

  test ("creates Transaction reference correctly", () => {
    testRegistration ("p", "domob", ALICE)
    testMove ("p", "domob", "{}")

    assertTransaction (TXID)

    const nameId = tokenIdToBytes (tokenIdForName ("p", "domob"))
    const moves = NameEntity.load (nameId)!.moves.load ()
    assert.i32Equals (moves.length, 1)
    assert.bytesEquals (moves[0].tx, TXID)
  })

  test ("creates Payments for moves", () => {
    testRegistration ("p", "domob", ALICE)
    testMove ("p", "domob", "{}")
    testMoveWithPayment ("p", "domob", "{}", 42, BOB)
    testMove ("p", "domob", "{}")
    testMoveWithPayment ("p", "domob", "{}", 50, ALICE)

    assertNumMoves ("p", "domob", 4)
    assertMovePayment ("p", "domob", 42, BOB)
    assertMovePayment ("p", "domob", 50, ALICE)
  })

  test ("handles Games in moves", () => {
    testRegistration ("p", "domob", ALICE)
    testRegistration ("p", "andy", BOB)

    testMove ("p", "domob", "{\"g\": {\"tn\": invalid}}")
    testMove ("p", "domob", "{\"g\": {\"tn\": 42}} extra")
    testMove ("p", "andy", "{\"g\": {\"xs\": 0, \"sv\": {}}}")

    assertGame ("sv")
    assertGame ("xs")
    assert.notInStore ("Game", Bytes.fromUTF8 ("tn").toHexString ())

    assertNumMoves ("p", "domob", 2)
    let nameId = tokenIdToBytes (tokenIdForName ("p", "domob"))
    let nameEntity = NameEntity.load (nameId)!
    const movesDomob = nameEntity.moves.load ()
    for (let i = 0; i < movesDomob.length; ++i)
      assert.i32Equals (movesDomob[i].games.load ().length, 0)

    assertNumMoves ("p", "andy", 1)
    nameId = tokenIdToBytes (tokenIdForName ("p", "andy"))
    nameEntity = NameEntity.load (nameId)!
    const moveAndy = nameEntity.moves.load ()[0]
    const games = moveAndy.games.load ()
    assert.i32Equals (games.length, 2)
    let foundXS = false
    let foundSV = false
    for (let i = 0; i < games.length; ++i)
      {
        assert.bytesEquals (games[i].move, moveAndy.id)
        const gameStr = GameEntity.load (games[i].game)!.game
        if (gameStr == "xs")
          {
            foundXS = true
            assert.stringEquals (games[i].gamemove, "0")
          }
        else if (gameStr == "sv")
          {
            foundSV = true
            assert.stringEquals (games[i].gamemove, "{}")
          }
      }
    assert.assertTrue (foundXS)
    assert.assertTrue (foundSV)
  })

})

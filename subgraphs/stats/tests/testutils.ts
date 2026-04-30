import {
  newMockEvent,
} from "matchstick-as/assembly/index"

import {
  Address,
  BigInt,
  Bytes,
  ethereum,
} from "@graphprotocol/graph-ts"

import {
  Move as MoveEvent,
  Registration as RegistrationEvent,
  Transfer as TransferEvent,
} from "../generated/XayaAccounts/IXayaAccounts"

import {
  handleMove,
  handleRegistration,
  handleTransfer,
} from "../src/handlers"

import {
  tokenIdForName,
} from "../src/util"

/* Some addresses to use in tests.  */
export const ALICE
    = Address.fromString ("0x4807cd9e01ca43f6d8b5bc70c59ff64a3433b778")
export const BOB
    = Address.fromString ("0x78ecbc2f0d748be27570a6c33e06458a10cebf8d")

/* Transaction id we use in tests.  */
export const TXID = Bytes.fromI32 (200)

/* Internal counter to produce unique logIndex values.  */
let nextLogIndex: i32 = 0

/**
 * Helper function to create a new mock event and also set some
 * dummy values (such as block / transaction / logIndex) on it.
 */
function mockEvent (): ethereum.Event
{
  const ev = newMockEvent ()

  ev.block.number = BigInt.fromI32 (1000)
  ev.block.timestamp = BigInt.fromI32 (100)
  ev.transaction.hash = TXID
  ev.logIndex = BigInt.fromI32 (nextLogIndex++)

  ev.parameters = new Array ()

  return ev
}

/**
 * Helper function to create and process a Move event with payment.
 */
export function testMoveWithPayment (ns: String, name: String, mv: String,
                                     amount: i32, receiver: Address): void
{
  const tokenId = tokenIdForName (ns, name)

  const ev = changetype<MoveEvent> (mockEvent ())
  ev.parameters.push (
      new ethereum.EventParam ("ns", ethereum.Value.fromString (ns)))
  ev.parameters.push (
      new ethereum.EventParam ("name", ethereum.Value.fromString (name)))
  ev.parameters.push (
      new ethereum.EventParam ("mv", ethereum.Value.fromString (mv)))
  ev.parameters.push (
      new ethereum.EventParam ("tokenId",
                               ethereum.Value.fromUnsignedBigInt (tokenId)))
  ev.parameters.push (
      new ethereum.EventParam ("nonce", ethereum.Value.fromI32 (42)))
  ev.parameters.push (
      new ethereum.EventParam ("mover",
                               ethereum.Value.fromAddress (Address.zero ())))
  ev.parameters.push (
      new ethereum.EventParam ("amount", ethereum.Value.fromI32 (amount)))
  ev.parameters.push (
      new ethereum.EventParam ("receiver",
                               ethereum.Value.fromAddress (receiver)))

  handleMove (ev)
}

/**
 * Helper function to create and process a Move event.
 */
export function testMove (ns: String, name: String, mv: String): void
{
  testMoveWithPayment (ns, name, mv, 0, Address.zero ())
}

/**
 * Helper function to create and process a Registration event.
 */
export function testRegistration (ns: String, name: String,
                                  owner: Address): void
{
  const tokenId = tokenIdForName (ns, name)

  const ev = changetype<RegistrationEvent> (mockEvent ())
  ev.parameters.push (
      new ethereum.EventParam ("ns", ethereum.Value.fromString (ns)))
  ev.parameters.push (
      new ethereum.EventParam ("name", ethereum.Value.fromString (name)))
  ev.parameters.push (
      new ethereum.EventParam ("tokenId",
                               ethereum.Value.fromUnsignedBigInt (tokenId)))
  ev.parameters.push (
      new ethereum.EventParam ("owner", ethereum.Value.fromAddress (owner)))

  handleRegistration (ev)
}

/**
 * Helper function to create and process a Transfer event.
 */
export function testTransfer (ns: String, name: String,
                              from: Address, to: Address): void
{
  const tokenId = tokenIdForName (ns, name)

  const ev = changetype<TransferEvent> (mockEvent ())
  ev.parameters.push (
      new ethereum.EventParam ("from", ethereum.Value.fromAddress (from)))
  ev.parameters.push (
      new ethereum.EventParam ("to", ethereum.Value.fromAddress (to)))
  ev.parameters.push (
      new ethereum.EventParam ("tokenId",
                               ethereum.Value.fromUnsignedBigInt (tokenId)))

  handleTransfer (ev)
}

import {
  Move as MoveEvent,
  Registration as RegistrationEvent,
  Transfer as TransferEvent,
} from "../generated/XayaAccounts/IXayaAccounts"

import {
  Address as AddressEntity,
  Game as GameEntity,
  GameMove as GameMoveEntity,
  Move as MoveEntity,
  Name as NameEntity,
  Namespace as NamespaceEntity,
  Payment as PaymentEntity,
  Registration as RegistrationEntity,
  Transaction as TransactionEntity,
} from "../generated/schema"

import {
  Address,
  BigInt,
  Bytes,
  JSONValue,
  JSONValueKind,
  ethereum,
  json,
} from "@graphprotocol/graph-ts"

import {
  processMove as processMoveProfile,
} from "./profile"

import {
  serialiseJson,
} from "./serialise"

import {
  tokenIdForName,
  tokenIdToBytes,
} from "./util"

/**
 * Constructs a unique ID for an event, based on txhash and log index.
 */
function uniqueIdForEvent (ev: ethereum.Event): Bytes
{
  return ev.transaction.hash.concatI32 (ev.logIndex.toI32 ())
}

/**
 * Creates a Transaction entity if it doesn't exist already.
 */
function maybeCreateTransaction (ev: ethereum.Event): Bytes
{
  const id = ev.transaction.hash
  if (TransactionEntity.load (id) == null)
    {
      const entity = new TransactionEntity (id)
      entity.height = ev.block.number
      entity.timestamp = ev.block.timestamp
      entity.save ()
    }
  return id
}

/**
 * Creates an Address entity if it doesn't exist already.
 */
function maybeCreateAddress (addr: Address): void
{
  if (AddressEntity.load (addr) == null)
    new AddressEntity (addr).save ()
}

/**
 * Creates a Game entity if it doesn't exist already.  Returns the Game instance
 * ID used.
 */
function maybeCreateGame (gid: string): Bytes
{
  const id = Bytes.fromUTF8 (gid)

  if (GameEntity.load (id) == null)
    {
      const entity = new GameEntity (id)
      entity.game = gid
      entity.save ()
    }

  return id
}

/**
 * Tries to parse a move value for a p/ name, and returns all game IDs
 * that it contains (if any), mapping to the game-specific part
 * of the move data JSON.  Returns an empty map in case of parsing errors or
 * anything else that is invalid.
 */
function getPlayerMoveGames (value: JSONValue): Map<string, string>
{
  const res = new Map<string, string> ()

  if (value.kind != JSONValueKind.OBJECT)
    return res
  let obj = value.toObject ()

  const valOrNull = obj.get ("g")
  if (valOrNull == null || valOrNull.kind != JSONValueKind.OBJECT)
    return res
  obj = valOrNull.toObject ()

  for (let i = 0; i < obj.entries.length; ++i)
    res.set (obj.entries[i].key, serialiseJson (obj.entries[i].value))

  return res
}

/**
 * Tries to parse a name's move as JSON and process it (if successful)
 * in various ways.
 */
function processMoveJson (mvEntity: MoveEntity, ns: string, name: string): void
{
  const parsed = json.try_fromString (mvEntity.move)
  if (parsed.isError)
    return

  if (ns == "p")
    {
      const games = getPlayerMoveGames (parsed.value)
      for (let i = 0; i < games.keys ().length; ++i)
        {
          const game = games.keys ()[i]
          const gid = maybeCreateGame (game);

          const gmvId = mvEntity.id.concat (gid)
          const gmvEntity = new GameMoveEntity (gmvId)
          gmvEntity.move = mvEntity.id
          gmvEntity.tx = mvEntity.tx
          gmvEntity.game = gid
          gmvEntity.gamemove = games.get (game)
          gmvEntity.save ()
        }
    }

  if (ns == "p" || ns == "g")
    {
      processMoveProfile (ns, name, parsed.value)
    }
}

export function handleMove (ev: MoveEvent): void
{
  const nsId = Bytes.fromUTF8 (ev.params.ns)
  const tokenId = tokenIdToBytes (ev.params.tokenId)
  const uniqueId = uniqueIdForEvent (ev)

  const mvEntity = new MoveEntity (uniqueId)
  mvEntity.tx = maybeCreateTransaction (ev)
  mvEntity.name = tokenId
  mvEntity.move = ev.params.mv
  mvEntity.save ()

  if (ev.params.receiver != Address.zero ())
    {
      const payment = new PaymentEntity (uniqueId)
      payment.move = mvEntity.id
      payment.receiver = ev.params.receiver
      payment.amount = ev.params.amount
      payment.save ()
    }

  processMoveJson (mvEntity, ev.params.ns, ev.params.name)
}

export function handleRegistration (ev: RegistrationEvent): void
{
  /* Sanity check to ensure our implementation of the tokenId computation
     is in sync with the smart contract.  */
  assert (ev.params.tokenId == tokenIdForName (ev.params.ns, ev.params.name))

  maybeCreateAddress (ev.params.owner)

  const nsId = Bytes.fromUTF8 (ev.params.ns)
  if (NamespaceEntity.load (nsId) == null)
    {
      const nsEntity = new NamespaceEntity (nsId)
      nsEntity.ns = ev.params.ns
      nsEntity.save ()
    }

  const tokenId = tokenIdToBytes (ev.params.tokenId)
  const nameEntity = new NameEntity (tokenId)
  nameEntity.ns = nsId
  nameEntity.name = ev.params.name
  nameEntity.owner = ev.params.owner
  nameEntity.save ()

  const regEntity = new RegistrationEntity (uniqueIdForEvent (ev))
  regEntity.tx = maybeCreateTransaction (ev)
  regEntity.name = tokenId
  regEntity.save ()
}

export function handleTransfer (ev: TransferEvent): void
{
  maybeCreateAddress (ev.params.to)

  const tokenId = tokenIdToBytes (ev.params.tokenId)
  const nameEntity = NameEntity.load (tokenId)

  /* When a name is registered, it is minted first (which Transfer's from
     the zero address), and later the Registration event is emitted.  In this
     case the name does not exist.  Ignore that, as the registration handler
     will set the owner.  */
  if (nameEntity == null)
    return

  nameEntity.owner = ev.params.to
  nameEntity.save ()
}

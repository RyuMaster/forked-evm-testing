import {
  Name as NameEntity,
  ProfileData as ProfileDataEntity,
} from "../generated/schema"

import {
  Bytes,
  JSONValue,
  JSONValueKind,
  store,
} from "@graphprotocol/graph-ts"

import {
  tokenIdForName,
  tokenIdToBytes,
} from "./util"

/** The game ID for the profile / xid game.  */
const gameId = "id"

/**
 * Returns the ProfileData entity ID for a given account token ID,
 * admin flag and key.
 */
export function profileDataId (accountId: Bytes, admin: boolean,
                               key: string): Bytes
{
  return accountId
      .concat (Bytes.fromHexString (admin ? "0x01" : "0x00"))
      .concat (Bytes.fromUTF8 (key))
}

/**
 * Applies a single key/value update (with null meaning delete) to the
 * ProfileData entity store for the given account.
 */
function applyProfileEntry (accountId: Bytes, admin: boolean,
                            key: string, value: JSONValue | null): void
{
  const id = profileDataId (accountId, admin, key)

  if (value == null || value.kind == JSONValueKind.NULL)
    {
      store.remove ("ProfileData", id.toHexString ())
      return
    }

  if (value.kind != JSONValueKind.STRING)
    return

  let entity = ProfileDataEntity.load (id)
  if (entity == null)
    {
      /* As a safety net preventing a bad internal link, we ignore any
         profile data for non-existing names (such as when created by
         an admin command).  */
      if (NameEntity.load (accountId) == null)
        return
      entity = new ProfileDataEntity (id)
      entity.account = accountId
      entity.admin = admin
      entity.key = key
    }
  entity.value = value.toString ()
  entity.save ()
}

/**
 * Processes a "set" or "admset" array from the profile move JSON.
 * For "set" entries each element has {"k": key, "v": value}.
 * For "admset" entries each element has {"u": user, "k": key, "v": value}.
 */
function processSetArray (entries: JSONValue, admin: boolean,
                          defaultAccountId: Bytes | null): void
{
  if (entries.kind != JSONValueKind.ARRAY)
    return
  const arr = entries.toArray ()

  for (let i = 0; i < arr.length; ++i)
    {
      const entry = arr[i]
      if (entry.kind != JSONValueKind.OBJECT)
        continue
      const obj = entry.toObject ()

      const kVal = obj.get ("k")
      if (kVal == null || kVal.kind != JSONValueKind.STRING)
        continue
      const key = kVal.toString ()

      const vVal = obj.get ("v")

      let accountId: Bytes
      if (admin)
        {
          const uVal = obj.get ("u")
          if (uVal == null || uVal.kind != JSONValueKind.STRING)
            continue
          applyProfileEntry (
              tokenIdToBytes (tokenIdForName ("p", uVal.toString ())),
              admin, key, vVal)
        }
      else
        applyProfileEntry (defaultAccountId!, admin, key, vVal)
    }
}

/**
 * Processes a move from a name (p/ or g/) that is already parsed as JSON
 * and updates the profile-data indexing in case it is relevant.
 */
export function processMove (ns: string, name: string, mv: JSONValue): void
{
  if (mv.kind != JSONValueKind.OBJECT)
    return

  if (ns == "p")
    {
      /* User move: {"g": {gameId: {"profile": {"set": [...]}}}} */
      const mvObj = mv.toObject ()
      const gVal = mvObj.get ("g")
      if (gVal == null || gVal.kind != JSONValueKind.OBJECT)
        return
      const gObj = gVal.toObject ()

      const gameVal = gObj.get (gameId)
      if (gameVal == null || gameVal.kind != JSONValueKind.OBJECT)
        return
      const gameObj = gameVal.toObject ()

      const profileVal = gameObj.get ("profile")
      if (profileVal == null || profileVal.kind != JSONValueKind.OBJECT)
        return
      const profileObj = profileVal.toObject ()

      const setVal = profileObj.get ("set")
      if (setVal == null)
        return

      const accountId = tokenIdToBytes (tokenIdForName ("p", name))
      processSetArray (setVal, false, accountId)
    }
  else if (ns == "g" && name == gameId)
    {
      /* Admin command: {"cmd": {"profile": {"admset": [...]}}} */
      const mvObj = mv.toObject ()
      const cmdVal = mvObj.get ("cmd")
      if (cmdVal == null || cmdVal.kind != JSONValueKind.OBJECT)
        return
      const cmdObj = cmdVal.toObject ()

      const profileVal = cmdObj.get ("profile")
      if (profileVal == null || profileVal.kind != JSONValueKind.OBJECT)
        return
      const profileObj = profileVal.toObject ()

      const admsetVal = profileObj.get ("admset")
      if (admsetVal == null)
        return

      processSetArray (admsetVal, true, null)
    }
}

#!/usr/bin/env python3

"""
JSON-RPC server that provides some helper / utility functionality for
testing Xaya applications with a forked EVM chain.
"""

import jsonrpclib
from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

import json
import os
import time

chainRpc = "http://nginx/chain"
eth = jsonrpclib.ServerProxy (chainRpc)
w3 = Web3 (Web3.HTTPProvider (chainRpc))
w3.middleware_onion.inject (ExtraDataToPOAMiddleware, layer=0)

gsp = jsonrpclib.ServerProxy ("http://nginx/gsp")


def loadAbi (nm):
  with open (os.path.join ("/abi", "%s.json" % nm), "rt") as f:
    data = json.load (f)
  return data["abi"]


erc20abi = loadAbi ("IERC20")

accounts = w3.eth.contract (address=os.getenv ("ACCOUNTS_CONTRACT"),
                            abi=loadAbi ("IXayaAccounts"))
wchi = w3.eth.contract (address=accounts.functions.wchiToken ().call (),
                        abi=erc20abi)

eth.anvil_autoImpersonateAccount (True)


################################################################################


def mineblock ():
  """Mines a block on the EVM chain."""
  eth.evm_mine ()


def mineblockat (timestamp):
  """Mines a block on the EVM chain at the given time."""
  eth.evm_mine (timestamp)


def setbalance (addr, wei):
  """Sets the Ether balance of the given address in Wei."""
  eth.anvil_setBalance (addr, wei)


def ensuregas (addr):
  """
  If the address has less than a minimum balance, increase the balance
  to the minimum to ensure it can pay for gas.
  """

  minBalance = "1"
  minWei = w3.to_wei (minBalance, "ether")

  if w3.eth.get_balance (addr, "latest") < minWei:
    setbalance (addr, minWei)


def transfertoken (token, sender, receiver, amount):
  """
  Transfers the given amount of some ERC20 token from the
  sender to the receiver address, using account impersonation.
  """

  ensuregas (sender)

  c = w3.eth.contract (address=token, abi=erc20abi)
  c.functions.transfer (receiver, amount).transact ({"from": sender})

  mineblock ()


def tryRegisterName (ns, name, receiver):
  """
  If the specified name does not exist, register it for the receiver and
  return True.  Otherwise (the name exists already), returns False.
  """

  if accounts.functions.exists (ns, name).call ():
    return False

  ensuregas (receiver)
  wchi.functions.approve (accounts.address, 2**256-1) \
      .transact ({"from": receiver})
  mineblock ()
  accounts.functions.register (ns, name).transact ({"from": receiver})
  mineblock ()

  return True


def getNameOwner (ns, name):
  """
  For an existing name, retrieve the owner address and return it along
  with the name's token ID.
  """

  tokenId = accounts.functions.tokenIdForName (ns, name).call ()
  owner = accounts.functions.ownerOf (tokenId).call ()

  return owner, tokenId


def getname (ns, name, receiver):
  """
  Gets the specified name into the receiver address.  If the name does not
  exist yet, it will be registered.  If it exists, then it will be transferred
  using address impersonation.

  Returns a dict with:
  - success: True/False
  - action: 'registered' or 'transferred'
  - owner: the new owner address
  """

  result = {
    "success": False,
    "ns": ns,
    "name": name,
    "receiver": receiver,
  }

  try:
    if tryRegisterName (ns, name, receiver):
      result["success"] = True
      result["action"] = "registered"
      result["owner"] = receiver
      return result

    owner, tokenId = getNameOwner (ns, name)
    result["previousOwner"] = owner
    result["tokenId"] = tokenId

    ensuregas (owner)
    accounts.functions.transferFrom (owner, receiver, tokenId) \
        .transact ({"from": owner})
    mineblock ()

    result["success"] = True
    result["action"] = "transferred"
    result["owner"] = receiver

  except Exception as e:
    result["error"] = str (e)
    result["errorType"] = type (e).__name__

  return result


def sendmove (ns, name, mv):
  """
  Sends a move with the given name without transferring it to another
  address.  The owner of the name is impersonated just to send the move
  itself.  If the name does not exist, this method fails.

  Returns a dict with:
  - success: True/False
  - txHash: transaction hash if successful
  - error: error message if failed
  - owner: the name owner address
  - moveData: the move data that was submitted
  """

  result = {
    "success": False,
    "ns": ns,
    "name": name,
  }

  try:
    if type (mv) != str:
      mv = json.dumps (mv, separators=(",", ":"))

    result["moveData"] = mv[:500]  # First 500 chars for debugging

    if not accounts.functions.exists (ns, name).call ():
      result["error"] = "name %s/%s does not exist yet" % (ns, name)
      return result

    owner, tokenId = getNameOwner (ns, name)
    result["owner"] = owner
    result["tokenId"] = tokenId

    ensuregas (owner)

    tx = accounts.functions.move (ns, name, mv, 2**256-1, 0, "0x" + "00" * 20) \
        .transact ({"from": owner})

    result["txHash"] = tx.hex () if hasattr (tx, 'hex') else str (tx)

    mineblock ()

    result["success"] = True
    result["message"] = "Move submitted successfully"

  except Exception as e:
    result["error"] = str (e)
    result["errorType"] = type (e).__name__

  return result


def validatecharacterstate (characterId, action):
  """
  Validates if a character can perform an action by checking the GSP state.

  Returns a dict with:
  - valid: True/False
  - error: error message if invalid
  - character: character state data
  """

  result = {
    "valid": False,
    "characterId": characterId,
    "action": action,
  }

  try:
    # Get characters from GSP
    chars = gsp.getcharacters ()
    # GSP returns characters under "data" key, not "characters"
    charData = chars.get ("data", [])

    # Find the character
    character = None
    for c in charData:
      if c.get ("id") == characterId:
        character = c
        break

    if character is None:
      result["error"] = "Character %d not found" % characterId
      return result

    result["character"] = {
      "id": character.get ("id"),
      "owner": character.get ("owner"),
      "pos": character.get ("pos"),
      "speed": character.get ("speed", 0),
      "inbuilding": character.get ("inbuilding"),
      "faction": character.get ("faction"),
    }

    # Check for move action
    if action == "move":
      # Check if character is in a building
      if character.get ("inbuilding") is not None:
        result["error"] = "Character is inside a building (id=%s). Must exit first." % character.get ("inbuilding")
        return result

      # Check if character has speed
      speed = character.get ("speed", 0)
      if speed <= 0:
        result["error"] = "Character has no movement speed (speed=%d)" % speed
        return result

      # Check if character is busy (has ongoing operation)
      if "ongoing" in character and character["ongoing"]:
        result["error"] = "Character is busy with ongoing operation"
        return result

      # Check movement field to see if already moving
      if character.get ("movement") and character["movement"].get ("partialstep", 0) > 0:
        result["warning"] = "Character is already moving, new waypoints will replace current path"

    result["valid"] = True
    result["message"] = "Character can perform %s action" % action

  except Exception as e:
    result["error"] = str (e)
    result["errorType"] = type (e).__name__

  return result


def syncgsp ():
  """
  Waits for the GSP to be synced up-to-date to the latest block of the
  basechain node.

  Returns a dict with:
  - success: True/False
  - blockhash: the synced block hash
  - waitTime: time spent waiting in seconds
  """

  result = {
    "success": False,
  }

  try:
    startTime = time.time ()

    blk = w3.eth.get_block ("latest")["hash"].hex ()
    if blk[:2] == "0x":
      blk = blk[2:]
    assert len (blk) == 64

    result["targetBlock"] = blk

    maxWait = 30  # Maximum wait time in seconds
    while True:
      state = gsp.getnullstate ()
      if state["state"] == "up-to-date" and state["blockhash"] == blk:
        break
      if time.time () - startTime > maxWait:
        result["error"] = "Timeout waiting for GSP sync after %d seconds" % maxWait
        result["lastState"] = state["state"]
        result["lastBlock"] = state.get ("blockhash", "unknown")
        return result
      time.sleep (0.1)

    result["success"] = True
    result["blockhash"] = blk
    result["waitTime"] = round (time.time () - startTime, 3)

  except Exception as e:
    result["error"] = str (e)
    result["errorType"] = type (e).__name__

  return result


################################################################################

srv = SimpleJSONRPCServer (('helper', 8000))

srv.register_function (mineblock)
srv.register_function (mineblockat)

srv.register_function (setbalance)
srv.register_function (ensuregas)

srv.register_function (transfertoken)

srv.register_function (getname)
srv.register_function (sendmove)
srv.register_function (validatecharacterstate)

srv.register_function (syncgsp)

srv.serve_forever ()

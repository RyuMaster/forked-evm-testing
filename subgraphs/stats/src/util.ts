import {
  BigInt,
  ByteArray,
  Bytes,
  crypto,
} from "@graphprotocol/graph-ts"

/**
 * Computes the XayaAccounts token ID for a namespace/name pair.  Note that this
 * matches the contract deployed on the Polygon network.
 */
export function tokenIdForName (ns: string, name: string): BigInt
{
  const hash = crypto.keccak256 (Bytes.fromUTF8 (ns + name))
  return BigInt.fromUnsignedBytes (changetype<ByteArray> (hash.reverse ()))
}

/**
 * Converts a token ID given as BigInt to Bytes that can be used
 * as ID in the subgraph.  The value will be exactly 32 bytes and in
 * little endian format.
 */
export function tokenIdToBytes (id: BigInt): Bytes
{
  const raw = ByteArray.fromBigInt (id)
  const padded = new Uint8Array (32)
  for (let i = 0; i < raw.length; i++)
    if (i < padded.length)
      padded[i] = raw[i]
    else
      assert (raw[i] == 0)
  return Bytes.fromUint8Array (padded)
}

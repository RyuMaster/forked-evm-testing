import {
  assert,
  describe,
  test,
} from "matchstick-as/assembly/index"

import {
  BigInt,
} from "@graphprotocol/graph-ts"

import {
  profileDataId as baseProfileDataId,
} from "../src/profile"

import {
  tokenIdForName,
  tokenIdToBytes,
} from "../src/util"

describe ("Token IDs", () => {

  test ("tokenIdForName", () => {
    /* We use comparison by decimal string instead of comparing the BigInt's
       since assert.bigIntEquals() cannot handle large unsigned values.  */
    assert.stringEquals (
        tokenIdForName ("p", "domob").toString (),
        "69102891577860888930938259306465286196644960499018480075295431222162104581274")
    assert.stringEquals (
        tokenIdForName ("p", "andy").toString (),
        "100424681252813573153174823809736495029176004014950528588886834976935835036043")
    assert.stringEquals (
        tokenIdForName ("g", "sv").toString (),
        "16060603809062330977548754223880688487019972341193984522649885074295654378662")
  })

  test ("tokenIdToBytes", () => {
    /* We need to make sure that conversion from token ID to bytes always
       produces exactly 32 bytes (64 hex characters).  */
    assert.stringEquals (
        tokenIdToBytes (BigInt.fromString (
            "123456"
        )).toHex (),
        "0x40e2010000000000000000000000000000000000000000000000000000000000")
    assert.stringEquals (
        tokenIdToBytes (BigInt.fromString (
            "115792089237316195423570985008687907853269984665640564039457584007913129639934"
        )).toHex (),
        "0xfeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
    assert.stringEquals (
        tokenIdToBytes (BigInt.fromString (
            "57896044618658097711785492504343953926634992332820282019728792003956564819967"
        )).toHex (),
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f")
  })

})

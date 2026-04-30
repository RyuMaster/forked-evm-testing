import {
  assert,
  describe,
  test,
} from "matchstick-as/assembly/index"

import {
  json,
} from "@graphprotocol/graph-ts"

import {
  serialiseJson,
} from "../src/serialise"

/* ************************************************************************** */

/**
 * Helper function that parses an input string to JSON, serialises it back,
 * and asserts that it is a full "roundtrip".
 */
function assertRoundtrip (data: String): void
{
  const value = json.fromString (data)
  const serialised = serialiseJson (value)
  assert.stringEquals (data, serialised)
}

/* ************************************************************************** */

describe ("serialiseJson", () => {

  test ("roundtrip tests", () => {
    assertRoundtrip ("[1,2,3]")
    assertRoundtrip ("[false,true,null,[],[42],[[]]]")
    assertRoundtrip ("{\"a\":true,\"b\":[1.5,-5,0.0],\"c\":{},\"d\":[]}")
    assertRoundtrip ("\"\\\\xäöü\\u0000\\\"\"")
    assertRoundtrip ("\"\\b\\f\\n\\r\\t\"")
  })

})

import {
  assert,
  afterEach,
  beforeEach,
  clearStore,
  describe,
  test,
} from "matchstick-as/assembly/index"

import {
  Bytes,
} from "@graphprotocol/graph-ts"

import {
  ALICE,
  testMove,
  testRegistration,
} from "./testutils"

import {
  profileDataId as baseProfileDataId,
} from "../src/profile"

import {
  tokenIdForName,
  tokenIdToBytes,
} from "../src/util"

/* ************************************************************************** */

/**
 * Computes the ProfileData entity ID for a p/ name, admin flag and key,
 * mirroring the logic in src/profile.ts.
 */
function profileDataId (name: String, admin: boolean, key: String): Bytes
{
  const accountId = tokenIdToBytes (tokenIdForName ("p", name))
  return baseProfileDataId (accountId, admin, key)
}

/**
 * Helper to assert a ProfileData entity exists with the given value string,
 * and that all its fields are set correctly.
 */
function assertProfileData (name: String, admin: boolean,
                             key: String, value: String): void
{
  const id = profileDataId (name, admin, key)
  const idHex = id.toHexString ()
  const accountId = tokenIdToBytes (tokenIdForName ("p", name))

  assert.fieldEquals ("ProfileData", idHex, "account", accountId.toHexString ())
  assert.fieldEquals ("ProfileData", idHex, "admin", admin ? "true" : "false")
  assert.fieldEquals ("ProfileData", idHex, "key", key)
  assert.fieldEquals ("ProfileData", idHex, "value", value)
}

/**
 * Helper to assert a ProfileData entity does not exist.
 */
function assertNoProfileData (name: String, admin: boolean, key: String): void
{
  assert.notInStore ("ProfileData", profileDataId (name, admin, key).toHexString ())
}

/* ************************************************************************** */

beforeEach (() => {
  testRegistration ("p", "andy", ALICE)
  testRegistration ("p", "domob", ALICE)
  testRegistration ("p", "foo bar", ALICE)
  testRegistration ("g", "id", ALICE)
  testRegistration ("g", "other", ALICE)
})

afterEach (clearStore)

describe ("User profile (p/ moves)", () => {

  test ("sets a key/value entry", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": \"domob\"}]}}}}")
    assertProfileData ("domob", false, "name", "domob")
  })

  test ("updates an existing entry", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": \"first\"}]}}}}")
    assertProfileData ("domob", false, "name", "first")

    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": \"second\"}]}}}}")
    assertProfileData ("domob", false, "name", "second")
  })

  test ("deletes an entry with null value", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": \"domob\"}]}}}}")
    assertProfileData ("domob", false, "name", "domob")

    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": null}]}}}}")
    assertNoProfileData ("domob", false, "name")
  })

  test ("last entry in array wins for same key", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"x\", \"v\": \"a\"}, {\"k\": \"x\", \"v\": \"b\"}]}}}}")
    assertProfileData ("domob", false, "x", "b")
  })

  test ("sets multiple keys independently", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"foo\", \"v\": \"1\"}, {\"k\": \"bar\", \"v\": \"2\"}]}}}}")
    assertProfileData ("domob", false, "foo", "1")
    assertProfileData ("domob", false, "bar", "2")
  })

  test ("does not mix entries across accounts", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"x\", \"v\": \"domob\"}]}}}}")
    testMove ("p", "andy",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"x\", \"v\": \"andy\"}]}}}}")
    testMove ("p", "foo bar",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"x\", \"v\": \"foo bar\"}]}}}}")
    assertProfileData ("domob", false, "x", "domob")
    assertProfileData ("andy", false, "x", "andy")
    assertProfileData ("foo bar", false, "x", "foo bar")
  })

  test ("ignores move targeting wrong game ID", () => {
    testMove ("p", "domob",
        "{\"g\": {\"other\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": \"domob\"}]}}}}")
    assertNoProfileData ("domob", false, "name")
  })

  test ("ignores malformed or irrelevant JSON structure", () => {
    testMove ("p", "domob", "{}")
    testMove ("p", "domob", "{\"g\": {}}")
    testMove ("p", "domob", "{\"g\": {\"id\": {}}}")
    testMove ("p", "domob", "{\"g\": {\"id\": {\"profile\": {}}}}")
    assertNoProfileData ("domob", false, "name")
  })

  test ("ignores non-string, non-null value for v", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": 42}]}}}}")
    assertNoProfileData ("domob", false, "name")
  })

  test ("does not create admin entries", () => {
    testMove ("p", "domob",
        "{\"g\": {\"id\": {\"profile\": {\"set\": [{\"k\": \"name\", \"v\": \"domob\"}]}}}}")
    assertProfileData ("domob", false, "name", "domob")
    assertNoProfileData ("domob", true, "name")
  })

})

describe ("Admin profile (g/ moves)", () => {

  test ("sets an admin entry for a user", () => {
    testMove ("g", "id",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"badge\", \"v\": \"gold\"}]}}}")
    assertProfileData ("domob", true, "badge", "gold")
  })

  test ("updates an existing admin entry", () => {
    testMove ("g", "id",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"badge\", \"v\": \"silver\"}]}}}")
    assertProfileData ("domob", true, "badge", "silver")

    testMove ("g", "id",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"badge\", \"v\": \"gold\"}]}}}")
    assertProfileData ("domob", true, "badge", "gold")
  })

  test ("deletes an admin entry with null value", () => {
    testMove ("g", "id",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"badge\", \"v\": \"gold\"}]}}}")
    assertProfileData ("domob", true, "badge", "gold")

    testMove ("g", "id",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"badge\", \"v\": null}]}}}")
    assertNoProfileData ("domob", true, "badge")
  })

  test ("sets entries for multiple users in one move", () => {
    testMove ("g", "id",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"x\", \"v\": \"1\"}, {\"u\": \"andy\", \"k\": \"x\", \"v\": \"2\"}, {\"u\": \"foo bar\", \"k\": \"x\", \"v\": \"3\"}]}}}")
    assertProfileData ("domob", true, "x", "1")
    assertProfileData ("andy", true, "x", "2")
    assertProfileData ("foo bar", true, "x", "3")
  })

  test ("does not create user entries", () => {
    testMove ("g", "id",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"badge\", \"v\": \"gold\"}]}}}")
    assertProfileData ("domob", true, "badge", "gold")
    assertNoProfileData ("domob", false, "badge")
  })

  test ("ignores g/ moves that are not the game ID", () => {
    testMove ("g", "other",
        "{\"cmd\": {\"profile\": {\"admset\": [{\"u\": \"domob\", \"k\": \"badge\", \"v\": \"gold\"}]}}}")
    assertNoProfileData ("domob", true, "badge")
  })

  test ("ignores malformed or irrelevant JSON structure", () => {
    testMove ("g", "id", "{}")
    testMove ("g", "id", "{\"cmd\": {}}")
    testMove ("g", "id", "{\"cmd\": {\"profile\": {}}}")
    assertNoProfileData ("domob", true, "badge")
  })

})

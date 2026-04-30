import {
  assert,
  afterEach,
  beforeEach,
  clearStore,
  createMockedFunction,
  describe,
  test,
} from "matchstick-as/assembly/index"

import {
  Address,
  BigInt,
  Bytes,
  crypto,
  ethereum,
} from "@graphprotocol/graph-ts"

import {
  Token,
  TokenPair,
  PriceObservation,
} from "../generated/schema"

import {
  USDC,
  WETH,
  WCHI,
  OBSERVATION_PERIOD,
  tokenPairId,
  getVirtualTimestamp,
  observationId,
  observeTokenPair,
} from "../src/uniswapPrices"

/* ************************************************************************** */

/**
 * Helper function to create a Token entity for testing.
 */
function createToken (addr: String, decimals: u8, symbol: String): void
{
  const token = new Token (Address.fromString (addr))
  token.decimals = decimals
  token.symbol = symbol
  token.save ()
}

/**
 * Helper function to create a TokenPair entity for testing.
 */
function createTokenPair (tradedToken: String, baseToken: String): void
{
  const tradedAddr = Address.fromString (tradedToken)
  const baseAddr = Address.fromString (baseToken)

  /* We create a dummy pair address by hashing the two token addresses
     together.  That is good enough for our tests.  */
  const hash = crypto.keccak256 (tradedAddr.concat (baseAddr))
  const pairAddr = Address.fromBytes (changetype<Bytes> (hash.slice (0, 20)))

  const pair = new TokenPair (tokenPairId (tradedAddr, baseAddr))
  pair.tradedToken = tradedAddr
  pair.baseToken = baseAddr
  pair.uniswapPair = pairAddr
  pair.save ()
}

/**
 * Helper function to set up fake Token and TokenPair entities
 * in the test store.
 */
function setupTokensAndPairs (): void
{
  createToken (USDC, 6, "USDC")
  createToken (WETH, 18, "WETH")
  createToken (WCHI, 8, "WCHI")

  createTokenPair (WETH, USDC)
  createTokenPair (WCHI, WETH)
}

/**
 * Sets up mock calls on a pair contract with the given
 * return values for reserves and cumulative prices.
 */
function mockTokenPair (tradedToken: Address, baseToken: Address,
                        cumPrice: BigInt,
                        tradedReserves: BigInt, baseReserves: BigInt,
                        lastTimestamp: i64): void
{
  const pairId = tokenPairId (tradedToken, baseToken)
  const pair = TokenPair.load (pairId)!
  const addr = Address.fromBytes (pair.uniswapPair)

  /* In the tests, we always pretend that the traded token is token1.  */
  createMockedFunction (addr, "token0", "token0():(address)")
      .returns ([ethereum.Value.fromAddress (baseToken)])
  createMockedFunction (addr, "price1CumulativeLast",
                        "price1CumulativeLast():(uint256)")
      .returns ([ethereum.Value.fromUnsignedBigInt (cumPrice)])
  createMockedFunction (addr, "getReserves",
                        "getReserves():(uint112,uint112,uint32)")
      .returns ([
        ethereum.Value.fromUnsignedBigInt (baseReserves),
        ethereum.Value.fromUnsignedBigInt (tradedReserves),
        ethereum.Value.fromUnsignedBigInt (BigInt.fromI64 (lastTimestamp)),
      ])
}

/**
 * Asserts that a price observation exists in the store with the
 * given data points.
 */
function assertPriceObservation (tradedToken: Address, baseToken: Address,
                                 virtualTime: i64, realTime: i64,
                                 cumulativePrice: BigInt,
                                 average24h: BigInt): void
{
  const id = observationId (tradedToken, baseToken, virtualTime)

  const obs = PriceObservation.load (id)!
  assert.bytesEquals (obs.pair, tokenPairId (tradedToken, baseToken))
  assert.bigIntEquals (obs.virtualTimestamp, BigInt.fromI64 (virtualTime))
  assert.bigIntEquals (obs.exactTimestamp, BigInt.fromI64 (realTime))
  assert.bigIntEquals (obs.cumulativePrice, cumulativePrice)
  if (average24h == BigInt.zero ())
    {
      /* For some reason, checking for null obs.average24h results in
         an internal error for AssemblyScript.  So we ignore that
         check for now.  */
      //assert.assertNull (obs.average24h)
    }
  else
    assert.bigIntEquals (obs.average24h!, average24h)
}

/* ************************************************************************** */

beforeEach (setupTokensAndPairs)
afterEach (clearStore)

describe ("UniswapPrices", () => {

  test ("getVirtualTimestamp", () => {
    assert.i32Equals (i32 (getVirtualTimestamp (OBSERVATION_PERIOD - 1)), 0)
    assert.i32Equals (i32 (getVirtualTimestamp (5 * OBSERVATION_PERIOD)),
                      i32 (5 * OBSERVATION_PERIOD))
    assert.i32Equals (i32 (getVirtualTimestamp (100 * OBSERVATION_PERIOD + 20)),
                      i32 (100 * OBSERVATION_PERIOD))
    assert.i32Equals (i32 (getVirtualTimestamp (42 * OBSERVATION_PERIOD - 1)),
                      i32 (41 * OBSERVATION_PERIOD))
  })

  test ("price observation", () => {
    const WCHIa = Address.fromString (WCHI)
    const WETHa = Address.fromString (WETH)

    const wchiDec = BigInt.fromI32 (10).pow (8)
    const wethDec = BigInt.fromI32 (10).pow (18)
    mockTokenPair (WCHIa, WETHa, BigInt.zero (),
                   wchiDec * BigInt.fromI32 (1000),
                   wethDec * BigInt.fromI32 (250),
                   OBSERVATION_PERIOD - 3)

    observeTokenPair (WCHIa, WETHa, OBSERVATION_PERIOD, OBSERVATION_PERIOD + 5)

    const cumPrice = BigInt.fromI32 (8) * BigInt.fromI32 (1).leftShift (110)
    assertPriceObservation (WCHIa, WETHa, OBSERVATION_PERIOD,
                            OBSERVATION_PERIOD + 5,
                            cumPrice, BigInt.zero ())
  })

  test ("24h average", () => {
    const WCHIa = Address.fromString (WCHI)
    const WETHa = Address.fromString (WETH)

    const wchiDec = BigInt.fromI32 (10).pow (8)
    const wethDec = BigInt.fromI32 (10).pow (18)

    mockTokenPair (WCHIa, WETHa, BigInt.zero (),
                   wchiDec * BigInt.fromI32 (250),
                   wethDec * BigInt.fromI32 (1000),
                   0)
    observeTokenPair (WCHIa, WETHa, 0, 1)

    mockTokenPair (WCHIa, WETHa, BigInt.zero (),
                   wchiDec * BigInt.fromI32 (500),
                   wethDec * BigInt.fromI32 (250),
                   OBSERVATION_PERIOD)
    observeTokenPair (WCHIa, WETHa, OBSERVATION_PERIOD, OBSERVATION_PERIOD + 3)

    const DAY = 3600 * 24
    mockTokenPair (WCHIa, WETHa, BigInt.zero (),
                   wchiDec * BigInt.fromI32 (250),
                   wethDec * BigInt.fromI32 (1000),
                   0)
    observeTokenPair (WCHIa, WETHa, DAY, DAY + 5)

    const cumPrice
        = BigInt.fromI32 (DAY + 5) * BigInt.fromI32 (4).leftShift (112)
    assertPriceObservation (WCHIa, WETHa, DAY, DAY + 5,
                            cumPrice, BigInt.fromI32 (4).leftShift (112))
  })

})

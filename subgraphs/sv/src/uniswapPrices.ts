import {
  Token,
  TokenPair,
  PriceObservation,
} from "../generated/schema"

import {
  IERC20Metadata,
} from "../generated/UniswapV2Factory/IERC20Metadata"
import {
  UniswapV2Factory,
} from "../generated/UniswapV2Factory/UniswapV2Factory"
import {
  UniswapV2Pair,
} from "../generated/UniswapV2Factory/UniswapV2Pair"

import {
  Address,
  BigInt,
  Bytes,
  dataSource,
  ethereum,
  log,
} from "@graphprotocol/graph-ts"

/* Token addresses that we track.  */
export const USDC = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
export const WETH = "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"
export const WCHI = "0xE79feAAA457ad7899357E8E2065a3267aC9eE601"

/* Interval (in seconds) at which to do observations.  The virtual timestamps
   attached to the observations will be multiples of this.  */
export const OBSERVATION_PERIOD: i64 = 3600

/* ************************************************************************** */

/**
 * Creates the ID of a TokenPair entity from the token addresses.
 */
export function tokenPairId (tradedToken: Address, baseToken: Address): Bytes
{
  return tradedToken.concat (baseToken)
}

/**
 * Creates a Token instance (if it doesn't exist yet) for the given address.
 */
function createToken (addr: Address): void
{
  let token = Token.load (addr)
  if (token != null)
    return

  const contract = IERC20Metadata.bind (addr)

  token = new Token (addr)
  token.decimals = contract.decimals ()
  token.symbol = contract.symbol ()
  token.save ()
}

/**
 * Creates the Token and TokenPair entities for a given pair of tokens.
 */
function createTokenPair (factory: UniswapV2Factory,
                          tradedToken: Address, baseToken: Address): void
{
  const pairId = tokenPairId (tradedToken, baseToken)
  let pair = TokenPair.load (pairId)
  if (pair != null)
    return

  createToken (tradedToken)
  createToken (baseToken)

  const pairAddr = factory.getPair (tradedToken, baseToken)
  if (pairAddr == Address.zero ())
    log.critical ("No Uniswap pair exists for tokens: {}, {}",
                  [tradedToken.toHexString (), baseToken.toHexString ()])

  pair = new TokenPair (pairId)
  pair.tradedToken = tradedToken
  pair.baseToken = baseToken
  pair.uniswapPair = pairAddr
  pair.save ()
}

/* ************************************************************************** */

/**
 * Finds the virtual timestamp corresponding to a given real block timestamp.
 */
export function getVirtualTimestamp (realTimestamp: i64): i64
{
  return realTimestamp - (realTimestamp % OBSERVATION_PERIOD)
}

/**
 * Constructs the entity ID for a price observation.
 */
export function observationId (tradedToken: Address, baseToken: Address,
                               virtualTime: i64): Bytes
{
  const pairId = tokenPairId (tradedToken, baseToken)

  /* We add the timestamp as I64 in big-endian byte order, so that sorting
     is done chronologically.  */
  let buffer = new ArrayBuffer (8)
  let view = new DataView (buffer)
  view.setInt64 (0, virtualTime, false)

  return pairId.concat (changetype<Bytes> (Uint8Array.wrap (buffer)))
}

/**
 * Calculates the 24h average price (if available) and adds it to
 * the existing PriceObservation entity.
 */
function addAveragePrice (tradedToken: Address, baseToken: Address,
                          virtualTime: i64): void
{
  const obsId = observationId (tradedToken, baseToken, virtualTime)
  const obs = PriceObservation.load (obsId)
  if (obs == null)
    return

  const prevVirtualTime = virtualTime - 3600 * 24
  const prevObsId = observationId (tradedToken, baseToken, prevVirtualTime)
  const prevObs = PriceObservation.load (prevObsId)
  if (prevObs == null)
    return

  if (prevObs.exactTimestamp >= obs.exactTimestamp)
    log.critical ("Previous observation timestamp is too late", [])
  let average24h = obs.cumulativePrice - prevObs.cumulativePrice
  average24h /= obs.exactTimestamp - prevObs.exactTimestamp

  obs.average24h = average24h
  obs.save ()
}

/**
 * Reads and records in store a price observation for the given token pair
 * at the current time.
 */
export function observeTokenPair (tradedToken: Address, baseToken: Address,
                                  virtualTime: i64, realTime: i64): void
{
  const tradedEntity = Token.load (tradedToken)!
  const baseEntity = Token.load (baseToken)!

  const pairId = tokenPairId (tradedToken, baseToken)
  const pair = TokenPair.load (pairId)!
  const uniPair = UniswapV2Pair.bind (Address.fromBytes (pair.uniswapPair))

  /* We use the logic from
     https://github.com/Uniswap/v2-periphery/blob/master/contracts/libraries/UniswapV2OracleLibrary.sol
     to get the cumulative price at the current realTime.  */
  const realTime32: u32 = u32 (realTime % 2**32)
  const use0 = (uniPair.token0 () == tradedToken)
  let priceCum: BigInt = (use0
      ? uniPair.price0CumulativeLast () : uniPair.price1CumulativeLast ())
  const reserves = uniPair.getReserves ()
  const lastTime32: u32 = reserves.get_blockTimestampLast ().toU32 ()
  /* We do not care about overflow, which will not happen for the next
     couple of decades anyway.  */
  if (lastTime32 > realTime32)
    log.critical ("Uniswap pair {} timestamp overflow",
                  [pair.uniswapPair.toHexString ()])
  if (lastTime32 < realTime32)
    {
      const tradedReserve = (use0
          ? reserves.get_reserve0 () : reserves.get_reserve1 ())
      const baseReserve = (use0
          ? reserves.get_reserve1 () : reserves.get_reserve0 ())
      const timeDiff = BigInt.fromU32 (realTime32 - lastTime32)
      const numerator = baseReserve.leftShift (112) * timeDiff
      priceCum += numerator / tradedReserve
    }

  /* Now adjust the price in UQ112x112 fixed-point format for the token
     decimals, so the actual fractional value is the real price in full
     token units.  What we compute in fixed-point math is:

     priceWithDecimals = priceCum * (decTraded / decBase)
  */
  const bigInt10 = BigInt.fromI32 (10)
  const decTraded = bigInt10.pow (u8 (tradedEntity.decimals))
  const decBase = bigInt10.pow (u8 (baseEntity.decimals))
  let priceWithDecimals = priceCum * decTraded.leftShift (112)
  priceWithDecimals /= decBase
  priceWithDecimals = priceWithDecimals.rightShift (112)

  const obsId = observationId (tradedToken, baseToken, virtualTime)
  const obs = new PriceObservation (obsId)
  obs.pair = pairId
  obs.virtualTimestamp = BigInt.fromI64 (virtualTime)
  obs.exactTimestamp = BigInt.fromI64 (realTime)
  obs.cumulativePrice = priceWithDecimals
  obs.save ()

  addAveragePrice (tradedToken, baseToken, virtualTime)
}

/* ************************************************************************** */

function createTokenPairs (block: ethereum.Block): void
{
  const factory = UniswapV2Factory.bind (dataSource.address ())
  createTokenPair (factory, Address.fromString (WETH),
                   Address.fromString (USDC))
  createTokenPair (factory, Address.fromString (WCHI),
                   Address.fromString (WETH))
}

export function maybeObservePrices (block: ethereum.Block): void
{
  const realTime = block.timestamp.toI64 ()
  const virtualTime = getVirtualTimestamp (realTime)

  const USDCa = Address.fromString (USDC)
  const WETHa = Address.fromString (WETH)
  const WCHIa = Address.fromString (WCHI)

  /* If the tokens themselves have not been created yet, run a one-time
     token creation step first.  */
  if (Token.load (WCHIa) == null)
    createTokenPairs (block)

  /* We check one of the token pairs to see if there is already an
     observation, and end the current block handler if there is.  */
  if (PriceObservation.load (observationId (WCHIa, WETHa, virtualTime)) != null)
    return

  observeTokenPair (WETHa, USDCa, virtualTime, realTime)
  observeTokenPair (WCHIa, WETHa, virtualTime, realTime)
}

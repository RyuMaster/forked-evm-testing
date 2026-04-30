import {
  SharesMinted as SharesMintedEvent,
} from "../generated/ClubMinter/ClubMinter"

import {
  ClubAdded as ClubAddedEvent,
  ClubRemoved as ClubRemovedEvent,
  ClubSalePaused as ClubSalePausedEvent,
  Paused as PausedEvent,
  Unpaused as UnpausedEvent,
  PacksBought as PacksBoughtEvent,
  PricingUpdated as PricingUpdatedEvent,
  SeedUpdated as SeedUpdatedEvent,
  SwappingPackSale,
} from "../generated/templates/PackSaleForShop/SwappingPackSale"

import {
  Pack,
  PackShareContent,
  PacksBought,
  PricingStep,
  SaleClub,
  SaleTier,
} from "../generated/schema"

import {
  PackSaleForShop,
} from "../generated/templates"

import {
  PACK_START_HEIGHT,
  SALE_CONTRACTS,
} from "./config"

import {
  Address,
  BigInt,
  Bytes,
  ValueKind,
  ethereum,
  log,
  store,
} from "@graphprotocol/graph-ts"

/* ************************************************************************** */

/**
 * Convert the clubId as i32 to the SaleClub entity ID (which is Bytes).
 */
function clubEntityId (clubId: i32): Bytes
{
  return Bytes.fromI32 (clubId)
}

/**
 * Removes all data stored about a given pack primary club.
 */
function removeClubsPack (clubId: i32): void
{
  const entityId = clubEntityId (clubId)
  const club = SaleClub.load (entityId)
  if (club == null)
    return

  const pack = Pack.load (entityId)
  if (pack == null)
    return

  const shares = pack.shares.load ()
  for (let i = 0; i < shares.length; ++i)
    store.remove ("PackShareContent", shares[i].id.toHexString ())
  store.remove ("Pack", entityId.toHexString ())
}

/**
 * Refreshes the pack data for the given primary club.
 */
function refreshClubsPack (clubId: i32): void
{
  removeClubsPack (clubId)

  const entityId = clubEntityId (clubId)
  const club = SaleClub.load (entityId)
  if (club == null)
    return

  /* This essentially checks if club.tier is null, and if so, returns, otherwise
     continues with the bytes value.  But directly doing that causes an
     internal compiler error with AssemblyScript.  */
  const tierValue = club.get ("tier")
  if (!tierValue || tierValue.kind == ValueKind.NULL)
    return
  const tier = tierValue.toBytes ()

  const contract = SwappingPackSale.bind (Address.fromBytes (tier))
  const maxPacks = contract.getMaxPacks (BigInt.fromI32 (clubId)).toI32 ()
  if (maxPacks <= 0)
    return

  const prev = contract.preview (BigInt.fromI32 (clubId), BigInt.fromI32 (1))

  const pack = new Pack (entityId)
  pack.primaryClub = entityId
  pack.maxPacks = maxPacks
  pack.cost = prev.cost
  pack.save ()

  for (let i = 0; i < prev.shares.length; ++i)
    {
      const thisClubId = clubEntityId (prev.shares[i].clubId.toI32 ())
      const sh = new PackShareContent (entityId.concat (thisClubId))
      sh.pack = entityId
      sh.club = thisClubId
      sh.num = prev.shares[i].numShares.toI32 ()
      sh.save ()
    }
}

/**
 * Refreshes a club's tranche / remainingInTranche data.
 */
function refreshClubTranche (clubId: i32): void
{
  const club = SaleClub.load (clubEntityId (clubId))
  if (club == null)
    return

  /* Same as in refreshClubsPack, we need to access the tier in this way
     to avoid an internal compiler error.  */
  const tierValue = club.get ("tier")
  if (!tierValue || tierValue.kind == ValueKind.NULL)
    {
      club.trancheIndex = -1
      club.remainingInTranche = 0
      club.save ()
      return
    }
  const tier = SaleTier.load (tierValue.toBytes ())
  if (tier == null)
    {
      club.trancheIndex = -1
      club.remainingInTranche = 0
      club.save ()
      return
    }
  const pricing = tier.pricingSteps.load ()

  /* Go through the pricing steps to find which one we are currently in.  Note
     that toTotal is the value at which the tranche is still in effect,
     one less than the total at which the next tranche applies.  */
  for (let i = 0; i < pricing.length; ++i)
    if (club.minted <= pricing[i].toTotal)
      {
        club.trancheIndex = i
        club.remainingInTranche = pricing[i].toTotal + 1 - club.minted
        club.save ()
        return
      }

  /* We didn't find any matching tranche.  */
  club.trancheIndex = -1
  club.remainingInTranche = 0
  club.save ()
}

/**
 * Refreshes the pack data for all clubs in a tier.
 */
function refreshTierPacks (addr: Address): void
{
  const tier = SaleTier.load (addr)!
  const clubs = tier.clubs.load ()

  /* We use warning level here since info is spammed with the batch writes
     that are done per block, so those would just drown in there.  They are not
     really "something going wrong", though.  */
  log.warning ("Starting full refresh of packs in tier: {}", [tier.name])
  for (let i = 0; i < clubs.length; ++i)
    {
      refreshClubsPack (clubs[i].clubId)
      refreshClubTranche (clubs[i].clubId)
    }
  log.warning ("Finished pack refresh for tier: {}", [tier.name])
}

/* ************************************************************************** */

export function handleSharesMinted (event: SharesMintedEvent): void
{
  const id = clubEntityId (event.params.clubId.toI32 ())
  let club = SaleClub.load (id)
  if (club == null)
    {
      club = new SaleClub (id)
      club.clubId = event.params.clubId.toI32 ()
      /* For now, fill in dummy values for the required tranche
         fields.  They will be updated below.  */
      club.trancheIndex = -1
      club.remainingInTranche = 0
    }
  club.minted = event.params.totalMinted.toI32 ()
  club.save ()

  /* Update the tranche information.  */
  refreshClubTranche (event.params.clubId.toI32 ())

  /* All packs that contain the club need to be updated.  */
  const packs = club.containedInPacks.load ()
  for (let i = 0; i < packs.length; ++i)
    {
      const pack = Pack.load (packs[i].pack)!
      const primaryClub = SaleClub.load (pack.primaryClub)!
      refreshClubsPack (primaryClub.clubId)
    }
}

export function handleClubAdded (event: ClubAddedEvent): void
{
  const id = clubEntityId (event.params.clubId.toI32 ())
  let club = SaleClub.load (id)
  if (club == null)
    {
      club = new SaleClub (id)
      club.clubId = event.params.clubId.toI32 ()
      club.minted = 0
      club.trancheIndex = -1
      club.remainingInTranche = 0
    }
  club.tier = event.address
  club.pausedInTier = null
  club.save ()

  if (event.block.number.toI32 () >= PACK_START_HEIGHT)
    refreshTierPacks (event.address)
}

export function handleClubRemoved (event: ClubRemovedEvent): void
{
  removeClubsPack (event.params.clubId.toI32 ())

  const club = SaleClub.load (clubEntityId (event.params.clubId.toI32 ()))!
  club.tier = null
  /* In the contract, ClubSalePaused is emitted after ClubRemoved.  So if the
     club is "removed" because it is paused, this is fine, and we will later
     set "pausedInTier" accordingly.  But the club might also be removed
     entirely.  */
  club.pausedInTier = null
  club.save ()

  if (event.block.number.toI32 () >= PACK_START_HEIGHT)
    refreshTierPacks (event.address)
}

export function handleClubSalePaused (event: ClubSalePausedEvent): void
{
  const club = SaleClub.load (clubEntityId (event.params.clubId.toI32 ()))!
  club.tier = null
  club.pausedInTier = event.address
  club.save ()
}

export function handleSalePaused (event: PausedEvent): void
{
  let tier = SaleTier.load (event.address)
  if (tier != null)
    {
      tier.active = false
      tier.save ()
    }
}

export function handleSaleUnpaused (event: UnpausedEvent): void
{
  let tier = SaleTier.load (event.address)
  if (tier != null)
    {
      tier.active = true
      tier.save ()
    }
}

export function handlePricingUpdated (event: PricingUpdatedEvent): void
{
  const tier = SaleTier.load (event.address)!

  /* Clear out all old pricing steps recorded.  */
  {
    const steps = tier.pricingSteps.load ()
    for (let i = 0; i < steps.length; ++i)
      store.remove ("PricingStep", tier.id.concatI32 (i).toHexString ())
  }

  /* Add the new pricing steps.  */
  const steps = event.params.steps
  let total = 0
  for (let i = 0; i < steps.length; ++i)
    {
      const step = new PricingStep (tier.id.concatI32 (i))
      step.tier = tier.id
      step.index = i
      step.numShares = steps[i].num.toI32 ()
      step.price = steps[i].price

      step.fromTotal = total
      total += steps[i].num.toI32 ()
      step.toTotal = total - 1

      step.save ()
    }

  if (event.block.number.toI32 () >= PACK_START_HEIGHT)
    refreshTierPacks (event.address)
}

export function handleSeedUpdated (event: SeedUpdatedEvent): void
{
  /* If the tier entity is not yet created, do so now.  This is "lazy
     initialisation" of the entity, because we can only initialise it once
     the contract has been deployed (not in the "once" handler).  Upon
     contract deployment, a first SeedUpdated event is emitted.  */

  let tier = SaleTier.load (event.address)
  if (tier == null)
    {
      const contract = SwappingPackSale.bind (event.address)
      tier = new SaleTier (event.address)
      tier.name = contract.tier ()
      tier.active = !contract.paused ()
      tier.save ()
    }

  if (event.block.number.toI32 () >= PACK_START_HEIGHT)
    refreshTierPacks (event.address)
}

export function handlePacksBought (event: PacksBoughtEvent): void
{
  const id = event.transaction.hash.concatI32 (event.logIndex.toI32 ())
  const ev = new PacksBought (id)
  ev.timestamp = event.block.timestamp
  ev.buyer = event.params.buyer
  ev.receiver = event.params.receiver
  ev.primaryClub = clubEntityId (event.params.primaryClubId.toI32 ())
  ev.tier = event.address
  ev.numPacks = event.params.numPacks.toI32 ()
  ev.usdSpent = event.params.cost
  ev.save ()
}

/* ************************************************************************** */

export function createPackSaleTiers (block: ethereum.Block): void
{
  SALE_CONTRACTS.forEach ((addr) => {
    PackSaleForShop.create (Address.fromString (addr));

    /* At this point in time, the contract has not actually been deployed.
       We will initialise the SaleTier instance (with its name from the
       contract) later when the seed-update event is received first.  */
  });
}

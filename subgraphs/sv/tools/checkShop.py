#!/usr/bin/env python3

"""
This Python script queries the shop data (packs) from GraphQL and compares
it to the on-chain data retrieved directly from the smart contracts.  This
can be used to test that the shop data is correctly represented in the graph.
"""

import eth_abi
from simple_multicall_v6 import Multicall
import requests
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

import argparse
import json
import logging
import os.path
import sys

DEFAULT_SUBGRAPH = "https://api.studio.thegraph.com/query/97741/soccerverse-stats/version/latest"

################################################################################

class ShopChecker:
  """
  Worker class for querying the graph and checking against on-chain data.
  """

  def __init__ (self, rpcUrl, graphUrl):
    self.log = logging.getLogger ("")

    self.w3 = Web3 (Web3.HTTPProvider (rpcUrl))
    self.w3.middleware_onion.inject (ExtraDataToPOAMiddleware, layer=0)
    self.log.info ("Connected to chain %d" % self.w3.eth.chain_id)

    self.mc = Multicall (self.w3, "polygon")

    basedir = os.path.abspath (os.path.dirname (__file__))
    with open (os.path.join (basedir, "SwappingPackSale.json"), "rt") as f:
      data = json.load (f)
    self.saleAbi = data["abi"]

    self.graph = graphUrl

  def queryGraph (self, query):
    """
    Runs a GraphQL query and returns the JSON data.
    """

    response = requests.post (self.graph, json={"query": query})
    try:
      data = response.json ()
    except Exception as exc:
      raise RuntimeError ("Error in GraphQL query: %s\n%s"
                            % (str (exc), response.text))

    if "errors" in data:
      raise RuntimeError ("GraphQL error:\n%s" % data["errors"])

    return data["data"]

  def queryTiers (self):
    """
    Queries for all tiers in the graph and returns the contract addresses.
    """

    data = self.queryGraph ("""{
      saleTiers {
        id
      }
    }""")

    return [t["id"] for t in data["saleTiers"]]

  def checkTier (self, tierId, batchSize):
    """
    Runs the check for all clubs in one tier.
    """

    addr = self.w3.to_checksum_address (tierId)
    sale = self.w3.eth.contract (abi=self.saleAbi, address=addr)
    self.log.info ("Checking tier '%s' at %s..."
                      % (sale.functions.tier ().call (), sale.address))

    processed = 0
    while True:
      packs = self.queryGraph ("""{
        saleClubs (first: %d, skip: %d,
                   orderBy: clubId, orderDirection: asc,
                   where: {tier: "%s"}) {
          clubId
          primaryPack {
            cost
            maxPacks
            shares {
              club { clubId }
              num
            }
          }
        }
      }""" % (batchSize, processed, tierId))
      self.checkClubBatch (sale, packs["saleClubs"])
      cur = len (packs["saleClubs"])
      processed += cur
      if cur < batchSize:
        break
      self.log.info ("  processed %d clubs" % processed)

  def checkClubBatch (self, sale, clubs):
    """
    Checks a batch of clubs returned from the subgraph against the
    on-chain data.  The Web3 Contract instance for the clubs' sales tier
    must be passed as parameter c.
    """

    def prepareCall (call):
      return (sale.address, call._encode_transaction_data ())

    calls = []
    for c in clubs:
      clubId = c["clubId"]
      calls.extend ([
        prepareCall (sale.functions.getMaxPacks (clubId)),
        prepareCall (sale.functions.preview (clubId, 1)),
      ])
    res = self.mc.call (calls)[1]

    def compareDicts (a, b):
      if len (a) != len (b):
        return False
      for key, value in a.items ():
        if key not in b:
          return False
        if b[key] != value:
          return False
      return True

    for i in range (0, len (clubs)):
      c = clubs[i]
      (maxPacks, ) = eth_abi.decode (["uint256"], res[2 * i])
      if maxPacks != int (c["primaryPack"]["maxPacks"]):
        self.log.error ("Mismatch for club %d: maxPacks" % clubId)
      (_, _, cost, _, shares, _) = eth_abi.decode (
          ["(uint256,uint256,uint256,uint256,(uint256,uint256)[],uint256[])"],
          res[2 * i + 1])[0]
      if cost != int (c["primaryPack"]["cost"]):
        self.log.error ("Mismatch for club %d: cost" % clubId)
      mintDictGraph = {}
      for sh in c["primaryPack"]["shares"]:
        mintDictGraph[sh["club"]["clubId"]] = sh["num"]
      mintDictChain = {}
      for clubId, num in shares:
        mintDictChain[clubId] = num
      if not compareDicts (mintDictGraph, mintDictChain):
        self.log.error ("Mismatch for club %d: shares" % clubId)

################################################################################

if __name__ == "__main__":
  logging.basicConfig (stream=sys.stderr, level=logging.INFO)

  desc = "Checks The Graph shop data against on-chain data"
  parser = argparse.ArgumentParser (description=desc)
  parser.add_argument ("--eth_rpc_url", default="https://polygon-node.xaya.io",
                       help="URL for the EVM JSON-RPC interface to use")
  parser.add_argument ("--subgraph_url", default=DEFAULT_SUBGRAPH,
                       help="URL for the subgraph")
  parser.add_argument ("--batch_size", type=int, default=100,
                       help="Number of clubs to query at once from the graph")
  args = parser.parse_args ()

  checker = ShopChecker (args.eth_rpc_url, args.subgraph_url)
  tiers = checker.queryTiers ()
  for t in tiers:
    checker.checkTier (t, args.batch_size)

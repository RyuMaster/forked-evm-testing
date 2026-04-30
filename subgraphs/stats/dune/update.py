#!/usr/bin/env python3
"""
Script to import data from The Graph subgraph to Dune Analytics.
Handles incremental sync of moves and game moves data.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import requests

# Query ID for "get latest timestamp"
QUERY_LATEST_TIMESTAMP = 5192941


class DuneClient:
  """Client for interacting with Dune Analytics API."""
  
  def __init__(self, apiKey: str):
    self.apiKey = apiKey
    self.baseUrl = "https://api.dune.com"
    self.headers = {
      "X-DUNE-API-KEY": apiKey,
      "Content-Type": "application/json"
    }

  def executeQuery (self, queryId: int) -> str:
    """Execute a Dune query and return the execution ID."""
    url = f"{self.baseUrl}/api/v1/query/{queryId}/execute"
    
    payload = {
      "performance": "medium"
    }
    
    response = requests.post (url, json=payload, headers=self.headers, timeout=30)
    response.raise_for_status ()
    
    data = response.json ()
    return data["execution_id"]
  
  def getExecutionResult (self, executionId: str, maxWaitTime: int = 300) -> Dict[str, Any]:
    """Get the result of a Dune query execution, waiting for completion."""
    url = f"{self.baseUrl}/api/v1/execution/{executionId}/results"
    
    startTime = time.time ()
    
    while True:
      response = requests.get (url, headers=self.headers, timeout=30)
      response.raise_for_status ()
      
      data = response.json ()
      state = data.get ("state")
      
      if state == "QUERY_STATE_COMPLETED":
        return data
      elif state == "QUERY_STATE_FAILED":
        raise Exception (f"Query execution failed: {data.get ('error', 'Unknown error')}")
      elif state in ["QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING"]:
        # Still running, wait and retry
        elapsed = time.time () - startTime
        if elapsed > maxWaitTime:
          raise Exception (f"Query execution timed out after {maxWaitTime} seconds")
        
        print (f"Query still running (state: {state}), waiting...")
        time.sleep (5)  # Wait 5 seconds before checking again
      else:
        raise Exception (f"Unknown query state: {state}")

  def insertData (self, namespace: str, tableName: str, data: List[Dict[str, Any]]) -> None:
    """Insert data into a Dune table using NDJSON format."""
    if not data:
      print (f"No data to insert into {namespace}.{tableName}")
      return
    
    url = f"{self.baseUrl}/api/v1/table/{namespace}/{tableName}/insert"
    
    # Convert data to NDJSON format (one JSON object per line)
    ndjsonData = "\n".join (json.dumps (row) for row in data)
    
    headers = {
      "X-DUNE-API-KEY": self.apiKey,
      "Content-Type": "application/x-ndjson"
    }
    
    try:
      response = requests.post (url, data=ndjsonData, headers=headers, timeout=60)
      response.raise_for_status ()
      
      print (f"Successfully inserted {len (data)} rows into {namespace}.{tableName}")
      
    except requests.exceptions.HTTPError as e:
      print (f"HTTP error inserting data into {namespace}.{tableName}: {e}")
      print (f"Response: {e.response.text}")
      raise
    except Exception as e:
      print (f"Error inserting data into {namespace}.{tableName}: {e}")
      raise

  def getLatestTimestamp (self, tableName: str) -> Optional[int]:
    """Get the timestamp of the latest entry in the specified Dune table."""
    try:
      print (f"Querying Dune for latest timestamp from {tableName}...")
      
      # Execute the predefined query
      executionId = self.executeQuery (QUERY_LATEST_TIMESTAMP)
      print (f"Query execution started with ID: {executionId}")
      
      # Wait for results
      result = self.getExecutionResult (executionId)
      
      # Extract the latest timestamp from results
      rows = result.get ("result", {}).get ("rows", [])
      if not rows:
        print ("No data found in Dune table")
        return None
      
      latestTime = rows[0].get ("latest_time")
      if latestTime is None:
        print ("No timestamp found in Dune table")
        return None
      
      # Convert datetime string to Unix timestamp
      if isinstance (latestTime, str):
        # Parse Dune timestamp format: '2022-10-20 10:57:01.000 UTC'
        # Remove 'UTC' suffix and milliseconds for parsing
        cleanedTime = re.sub (r'\.\d{3}\s+UTC$', '', latestTime)
        dt = datetime.strptime (cleanedTime, '%Y-%m-%d %H:%M:%S')
        # Treat as UTC
        dt = dt.replace (tzinfo=timezone.utc)
        timestamp = int (dt.timestamp ())
      else:
        # Assume it's already a timestamp
        timestamp = int (latestTime)
      
      print (f"Latest timestamp from Dune: {timestamp} ({datetime.fromtimestamp (timestamp, tz=timezone.utc)})")
      return timestamp
      
    except Exception as e:
      print (f"Error getting latest timestamp from {tableName}: {e}")
      print ("Assuming no existing data and starting from beginning")
      return None


class SubgraphClient:
  """Client for querying The Graph subgraph."""
  
  def __init__(self, endpoint: str):
    self.endpoint = endpoint

  def queryMoves (self, afterTimestamp: Optional[int], batchSize: int, skip: int = 0) -> Dict[str, Any]:
    """Query moves and their associated game moves from the subgraph."""
    
    # Build the where clause for timestamp filtering
    whereClause = ""
    if afterTimestamp is not None:
      whereClause = f'where: {{timestamp_gt: "{afterTimestamp}"}}, '
    
    query = f"""
    query GetMoves($first: Int!, $skip: Int!) {{
      moves(
        {whereClause}
        first: $first,
        skip: $skip,
        orderBy: timestamp,
        orderDirection: asc
      ) {{
        id
        timestamp
        txid
        name {{
          name
        }}
        games {{
          id
          game {{
            game
          }}
        }}
      }}
    }}
    """
    
    variables = {
      "first": batchSize,
      "skip": skip
    }
    
    payload = {
      "query": query,
      "variables": variables
    }
    
    try:
      response = requests.post (self.endpoint, json=payload, timeout=30)
      response.raise_for_status ()
      
      data = response.json ()
      if "errors" in data:
        raise Exception (f"GraphQL errors: {data['errors']}")
      
      return data["data"]
      
    except Exception as e:
      print (f"Error querying subgraph: {e}")
      raise


class DataSyncer:
  """Main class that handles the data synchronization process."""
  
  def __init__(self, duneApiKey: str, subgraphEndpoint: str, graphqlBatchSize: int, maxMoves: int, insertBatchSize: int):
    self.duneClient = DuneClient (duneApiKey)
    self.subgraphClient = SubgraphClient (subgraphEndpoint)
    self.graphqlBatchSize = graphqlBatchSize
    self.maxMoves = maxMoves
    self.insertBatchSize = insertBatchSize
    
    # Storage for retrieved data
    self.moves: List[Dict[str, Any]] = []
    self.gameMoves: List[Dict[str, Any]] = []

  def getLatestTimestamp (self) -> Optional[int]:
    """Get the latest timestamp from Dune tables."""
    return self.duneClient.getLatestTimestamp ("xaya.moves")

  def postprocessLimitedData (self) -> None:
    """Remove moves with the latest timestamp when we hit the max moves limit."""
    if not self.moves:
      return
    
    # Find the latest timestamp among retrieved moves
    latestTimestamp = max (move["raw_timestamp"] for move in self.moves)
    
    print (f"Postprocessing: removing moves with latest timestamp {latestTimestamp}")
    
    # Get move IDs that have the latest timestamp (these will be removed)
    moveIdsToRemove = set ()
    for move in self.moves:
      if move["raw_timestamp"] == latestTimestamp:
        moveIdsToRemove.add (move["id"])
    
    # Remove moves with latest timestamp
    originalMoveCount = len (self.moves)
    self.moves = [move for move in self.moves if move["raw_timestamp"] != latestTimestamp]
    
    # Remove associated game moves
    originalGameMoveCount = len (self.gameMoves)
    self.gameMoves = [gm for gm in self.gameMoves if gm["move_id"] not in moveIdsToRemove]
    
    # Remove raw_timestamp field from remaining moves (not needed for Dune)
    for move in self.moves:
      del move["raw_timestamp"]
    
    removedMoves = originalMoveCount - len (self.moves)
    removedGameMoves = originalGameMoveCount - len (self.gameMoves)
    
    print (f"Removed {removedMoves} moves and {removedGameMoves} game moves with timestamp {latestTimestamp}")

  def retrieveAllData (self, afterTimestamp: Optional[int]) -> None:
    """Retrieve all moves and game moves from the subgraph after the given timestamp."""
    print (f"Starting data retrieval from subgraph...")
    print (f"Max moves limit: {self.maxMoves}")
    if afterTimestamp:
      print (f"Fetching moves after timestamp: {afterTimestamp}")
    else:
      print (f"Fetching all moves (no existing data found)")
    
    skip = 0
    totalMoves = 0
    totalGameMoves = 0
    reachedLimit = False
    
    while True:
      print (f"Fetching batch starting at skip={skip}, batch_size={self.graphqlBatchSize}")
      
      try:
        data = self.subgraphClient.queryMoves (afterTimestamp, self.graphqlBatchSize, skip)
        moves = data.get ("moves", [])
        
        if not moves:
          print ("No more moves to fetch")
          break
        
        # Process the moves
        for move in moves:
          # Check if we've reached the max moves limit
          if totalMoves >= self.maxMoves:
            print (f"Reached max moves limit of {self.maxMoves}")
            reachedLimit = True
            break
            
          # Convert timestamp to UTC datetime for Dune
          timestamp = int (move["timestamp"])
          timestampDt = datetime.fromtimestamp (timestamp, tz=timezone.utc)
          
          # Extract move data
          moveData = {
            "id": move["id"],
            "timestamp": timestampDt.isoformat (),
            "txid": move["txid"],
            "name": move["name"]["name"],
            "raw_timestamp": timestamp  # Keep raw timestamp for postprocessing
          }
          self.moves.append (moveData)
          totalMoves += 1
          
          # Extract associated game moves
          for gameMove in move.get ("games", []):
            gameMoveData = {
              "move_id": move["id"],
              "game": gameMove["game"]["game"]
            }
            self.gameMoves.append (gameMoveData)
            totalGameMoves += 1
        
        print (f"Processed {len (moves)} moves from this batch")
        
        # If we reached the limit, break out of the main loop
        if reachedLimit:
          break
        
        # If we got fewer moves than requested, we're done
        if len (moves) < self.graphqlBatchSize:
          print ("Reached end of available data")
          break
        
        skip += self.graphqlBatchSize
        
      except Exception as e:
        print (f"Error during data retrieval: {e}")
        raise
    
    # Postprocessing: if we reached the limit, drop moves with latest timestamp
    if reachedLimit:
      self.postprocessLimitedData ()
    
    print (f"Data retrieval complete:")
    print (f"  Total moves: {len (self.moves)}")
    print (f"  Total game moves: {len (self.gameMoves)}")

  def createTimestampBatches (self, moves: List[Dict[str, Any]], batchSize: int) -> List[List[Dict[str, Any]]]:
    """Create batches ensuring all moves with the same timestamp stay together."""
    if not moves:
      return []
    
    batches = []
    currentBatch = []
    currentBatchSize = 0
    currentTimestamp = None
    
    for move in moves:
      moveTimestamp = move["timestamp"]
      
      # If this is a new timestamp and we would exceed batch size, start new batch
      if (currentTimestamp is not None and 
          moveTimestamp != currentTimestamp and 
          currentBatchSize >= batchSize):
        
        if currentBatch:
          batches.append (currentBatch)
          currentBatch = []
          currentBatchSize = 0
      
      currentBatch.append (move)
      currentBatchSize += 1
      currentTimestamp = moveTimestamp
    
    # Add final batch if not empty
    if currentBatch:
      batches.append (currentBatch)
    
    return batches

  def insertAllData (self) -> None:
    """Insert all retrieved data into Dune in properly batched format."""
    if not self.moves and not self.gameMoves:
      print ("No data to insert")
      return
    
    print (f"Starting data insertion into Dune...")
    print (f"Insert batch size: {self.insertBatchSize}")
    
    # Create batches of moves ensuring timestamp consistency
    moveBatches = self.createTimestampBatches (self.moves, self.insertBatchSize)
    
    print (f"Created {len (moveBatches)} move batches")
    
    totalInsertedMoves = 0
    totalInsertedGameMoves = 0
    
    for i, moveBatch in enumerate (moveBatches):
      print (f"Processing batch {i + 1}/{len (moveBatches)} ({len (moveBatch)} moves)...")
      
      # Get move IDs in this batch
      moveIdsInBatch = {move["id"] for move in moveBatch}
      
      # Find all game moves associated with moves in this batch
      gameMovesForBatch = [
        gm for gm in self.gameMoves 
        if gm["move_id"] in moveIdsInBatch
      ]
      
      print (f"  Found {len (gameMovesForBatch)} associated game moves")
      
      try:
        # First insert the game moves for this batch
        if gameMovesForBatch:
          self.duneClient.insertData ("xaya", "game_moves", gameMovesForBatch)
          totalInsertedGameMoves += len (gameMovesForBatch)
        
        # Then insert the moves (remove raw_timestamp if it exists)
        movesToInsert = []
        for move in moveBatch:
          moveToInsert = move.copy ()
          if "raw_timestamp" in moveToInsert:
            del moveToInsert["raw_timestamp"]
          movesToInsert.append (moveToInsert)
        
        self.duneClient.insertData ("xaya", "moves", movesToInsert)
        totalInsertedMoves += len (movesToInsert)
        
        print (f"  Batch {i + 1} completed successfully")
        
      except Exception as e:
        print (f"Error inserting batch {i + 1}: {e}")
        print ("Stopping insertion process")
        raise
    
    print (f"Data insertion complete!")
    print (f"  Total moves inserted: {totalInsertedMoves}")
    print (f"  Total game moves inserted: {totalInsertedGameMoves}")

  def run (self) -> None:
    """Main execution method."""
    print ("Starting Dune data import process...")
    
    # Step 1: Get latest timestamp from Dune
    print ("Step 1: Getting latest timestamp from Dune...")
    latestTimestamp = self.getLatestTimestamp ()
    
    # Step 2: Retrieve all data from subgraph
    print ("Step 2: Retrieving data from subgraph...")
    self.retrieveAllData (latestTimestamp)
    
    print ("Data retrieval phase complete!")
    print (f"Retrieved {len (self.moves)} moves and {len (self.gameMoves)} game moves")
    
    # Step 3: Insert data into Dune
    if self.moves or self.gameMoves:
      print ("Step 3: Inserting data into Dune...")
      self.insertAllData ()
    else:
      print ("Step 3: No new data to insert")


def main ():
  """Main entry point."""
  parser = argparse.ArgumentParser (
    description="Import data from The Graph subgraph to Dune Analytics"
  )
  
  parser.add_argument (
    "--dune-api-key",
    required=True,
    help="Dune Analytics API key"
  )
  
  parser.add_argument (
    "--subgraph-endpoint",
    required=True,
    help="The Graph subgraph GraphQL endpoint URL"
  )
  
  parser.add_argument (
    "--graphql-batch-size",
    type=int,
    default=100,
    help="Batch size for GraphQL queries (default: 100)"
  )
  
  parser.add_argument (
    "--insert-batch-size",
    type=int,
    default=10000,
    help="Batch size for Dune insertions (default: 10000)"
  )
  
  parser.add_argument (
    "--max-moves",
    type=int,
    default=10000,
    help="Maximum number of moves to retrieve (default: 10000)"
  )
  
  args = parser.parse_args ()
  
  try:
    syncer = DataSyncer (
      duneApiKey=args.dune_api_key,
      subgraphEndpoint=args.subgraph_endpoint,
      graphqlBatchSize=args.graphql_batch_size,
      maxMoves=args.max_moves,
      insertBatchSize=args.insert_batch_size
    )
    
    syncer.run ()
    
  except KeyboardInterrupt:
    print ("\nOperation cancelled by user")
    sys.exit (1)
  except Exception as e:
    print (f"Error: {e}")
    sys.exit (1)


if __name__ == "__main__":
  main ()

#!/bin/sh -e

# Create a query with a given set of data and API key.  The data to pass
# to Dune's /query endpoint must be given as CLI argument, and can be one
# of the query-*.json files provided.

curl --request POST \
  --url https://api.dune.com/api/v1/query \
  --header "X-DUNE-API-KEY: ${APIKEY}" \
  --header "Content-Type: application/json" \
  --data "`cat $1`"

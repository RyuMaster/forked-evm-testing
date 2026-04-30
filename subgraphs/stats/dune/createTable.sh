#!/bin/sh -e

# Create a table with a given set of data and API key.  The data to pass
# to Dune's /create endpoint must be given as CLI argument, and can be one
# of the schema-*.json files provided.

curl --request POST \
  --url https://api.dune.com/api/v1/table/create \
  --header "X-DUNE-API-KEY: ${APIKEY}" \
  --header "Content-Type: application/json" \
  --data "`cat $1`"

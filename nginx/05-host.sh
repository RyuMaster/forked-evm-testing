#!/bin/sh
export BLOCKCHAIN_HOST=$(echo "$BLOCKCHAIN_ENDPOINT" | sed -E 's#^https?://([^/]+).*#\1#')

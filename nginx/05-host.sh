#!/bin/sh
export BLOCKCHAIN_HOST=$(echo "$BLOCKCHAIN_ENDPOINT" | sed -E 's#^https?://([^/?]+).*#\1#')
export BLOCKCHAIN_ORIGIN=$(echo "$BLOCKCHAIN_ENDPOINT" | sed -E 's#^(https?://[^/?]+).*#\1#')
export BLOCKCHAIN_PATH=$(echo "$BLOCKCHAIN_ENDPOINT" | sed -E 's#^https?://[^/?]+##; s#^$#/#; s#^\?#/?#')

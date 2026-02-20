#!/bin/sh
# Substitute only our custom env vars, preserving nginx's own $variables
envsubst '$BLOCKCHAIN_RPC_URL $BLOCKCHAIN_AUTH_HEADER' \
  < /proxy.conf.template \
  > /etc/nginx/conf.d/proxy.conf

exec nginx -g 'daemon off;'

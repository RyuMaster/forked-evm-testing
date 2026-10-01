# Builds an image based on the standard nginx but with our custom configuration,
# which runs a reverse proxy for the forked blockchain node and our
# utilities RPC server -- plus the tunnel gate (app key + njs method allowlist,
# native spec 2026-10-01-play-internal-test §3.2).

FROM nginx:stable-alpine

RUN rm /etc/nginx/conf.d/default.conf \
    && sed -i '1i load_module modules/ngx_http_js_module.so;' /etc/nginx/nginx.conf
COPY proxy.conf.template /etc/nginx/templates/default.conf.template
COPY rpc_allow.js /etc/nginx/njs/rpc_allow.js
COPY rpc_body.conf /etc/nginx/rpc_body.conf
COPY 04-require-key.sh /docker-entrypoint.d/04-require-key.sh
RUN chmod +x /docker-entrypoint.d/04-require-key.sh

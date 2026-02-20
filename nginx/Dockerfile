# Builds an image based on the standard nginx but with our custom configuration,
# which runs a reverse proxy for the blockchain node RPC and the GSP.

FROM nginx:stable-alpine

RUN rm /etc/nginx/conf.d/default.conf
COPY proxy.conf.template /proxy.conf.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

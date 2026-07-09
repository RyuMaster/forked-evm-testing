# This builds a Docker image that runs a forked EVM network via anvil.

FROM ghcr.io/foundry-rs/foundry

COPY entrypoint.sh /usr/local/bin/

# The base image runs as the non-root `foundry` user (uid 1000). Pre-create the
# state-snapshot dir owned by that user so the `basechain_state` volume mounted
# there (see docker-compose.yml) inherits writable ownership on first init —
# otherwise the fresh volume is root-owned and anvil (running as foundry) cannot
# write anvil-state.json and state persistence silently fails.
USER root
RUN mkdir -p /state && chown 1000:1000 /state
USER foundry

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

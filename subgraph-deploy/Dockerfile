FROM node:20-alpine

# psql lets us read the IPFS hash directly from graph-node's deployment_schemas
# table (more robust than parsing graph-cli stdout).
RUN apk add --no-cache postgresql-client gettext

# Pin graph-cli. Keep aligned with subgraphs' devDependency.
RUN npm install -g @graphprotocol/graph-cli@0.98.1

WORKDIR /work

# Build context MUST be the repo root so subgraphs/ is reachable. Build with:
#   docker build -f subgraph-deploy/Dockerfile -t subgraph-deploy .
# Subgraphs are baked into the image (not bind-mounted) so the stack works on
# Portainer agents that don't clone the git repo to the host.
COPY subgraphs /subgraphs

COPY subgraph-deploy/deploy.sh /usr/local/bin/deploy.sh
RUN chmod +x /usr/local/bin/deploy.sh

ENTRYPOINT ["/usr/local/bin/deploy.sh"]

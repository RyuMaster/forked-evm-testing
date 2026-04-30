FROM node:20-alpine

# psql lets us read the IPFS hash directly from graph-node's deployment_schemas
# table (more robust than parsing graph-cli stdout).
RUN apk add --no-cache postgresql-client

# Pin graph-cli. Keep aligned with subgraphs' devDependency.
RUN npm install -g @graphprotocol/graph-cli@0.98.1

WORKDIR /work
COPY deploy.sh /usr/local/bin/deploy.sh
RUN chmod +x /usr/local/bin/deploy.sh

ENTRYPOINT ["/usr/local/bin/deploy.sh"]

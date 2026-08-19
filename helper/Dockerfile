# This is a Docker container that runs a "helper" JSON-RPC server in
# Python that provides utility methods for interacting with the
# blockchain, such as mining blocks on demand or sending moves on behalf
# of arbitrary impersonated accounts.

# Use the node image to install npm packages, out of which we extract
# some contract ABI files.
FROM node AS npm
RUN mkdir /packages
WORKDIR /packages
RUN npm install @openzeppelin/contracts @xaya/eth-account-registry

FROM python:3-slim

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir jsonrpclib-pelix web3

WORKDIR /abi
COPY --from=npm \
  /packages/node_modules/@openzeppelin/contracts/build/contracts/IERC20.json \
  /packages/node_modules/@xaya/eth-account-registry/build/contracts/IXayaAccounts.json \
  .

WORKDIR /usr/local/bin
# warmup.py is not this image's entrypoint. The compose stack runs it as a
# separate one-shot `warmup` service off the same image (it needs exactly what
# the helper needs: web3 plus the ABIs above) and makes the helper wait for it.
COPY rpcserver.py warmup.py .

ENTRYPOINT ["/usr/local/bin/rpcserver.py"]

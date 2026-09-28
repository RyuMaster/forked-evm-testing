# Taurion read-scaling fanout

The taurionui fanout is one reader on the GSP: it uses `waitforchange` and makes one coherent world pull per block at a single block hash, then pushes a snapshot followed by small per-block deltas to every client over one SSE stream. It also reads each block's XayaAccounts `Move` logs and publishes the same-block `movementRoutes` needed for confirmed movement playback.

Build the pinned, unmodified taurionui image on the laptop:

```sh
sh fanout/build-image.sh
```

The Compose service runs with `GSP_RPC=http://gsp:8600`, `POLYGON_RPC=http://basechain:8545`, `FEED_PORT=8090`, and `FEED_ORIGIN=*`. Nginx exposes its endpoints under `/feed/`, including `/feed/stream?v=2`, `/feed/snapshot`, and `/feed/account?name=`. Live uses `https://feed.taurion.io`.

The GSP must carry the fork patch that puts `custom.superblock` in every envelope (`sources/gsp`, sub-project 2 Task 1); otherwise the fanout publishes `superblock: null`.

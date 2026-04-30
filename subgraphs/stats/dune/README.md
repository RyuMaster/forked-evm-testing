# Data Link to Dune Analytics

[Dune Analytics](https://dune.com/) does not support querying subgraphs
directly, and their built-in support for EVM events is not sufficient
to fully capture Xaya stats (as it would not support the per-game data
that we get from parsing the move JSON).  This folder contains scripts to
periodically run a "manual upload" of data from the subgraph into a Dune
custom table.

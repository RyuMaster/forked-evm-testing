# File: updaters/price_updater.py
import logging
import struct
import requests
from datetime import datetime

from config import SVC_POLYGON_SUBGRAPH_URL, SV_SUBGRAPH_URL
from db_manager import CHARSET, COLLATION

# 2**112, used to decode Uniswap UQ112x112 fixed-point price values.
_UQ112 = 2 ** 112


class PriceUpdater:
    def __init__(self, db_manager):
        self.db = db_manager
        self.component_name = "price_updater"
        self.svc_polygon_subgraph_url = SVC_POLYGON_SUBGRAPH_URL
        self.sv_subgraph_url = SV_SUBGRAPH_URL

        # Cache of hour_ts -> wchi_usdc (or None if unavailable).
        self._price_cache = {}

        # Fetch token pair IDs from the SV subgraph once at startup.
        self._wchi_weth_pair_id = None
        self._weth_usdc_pair_id = None
        self._init_token_pairs()

    ##########################################################################
    # SV subgraph price lookups
    ##########################################################################
    def _init_token_pairs(self):
        """
        Query the SV subgraph for WCHI, WETH, USDC token addresses and
        derive the two pair IDs we need for price lookups.
        """
        query = """
        {
          tokens(where: { symbol_in: ["WCHI", "WETH", "USDC"] }) {
            id
            symbol
          }
        }
        """
        try:
            resp = requests.post(self.sv_subgraph_url, json={"query": query}, timeout=10)
            js = resp.json()
            if "errors" in js:
                raise Exception(f"GraphQL error: {js['errors']}")

            addrs = {}
            for token in js["data"]["tokens"]:
                # id is the contract address as a hex string (with 0x prefix)
                addrs[token["symbol"]] = token["id"]

            for sym in ("WCHI", "WETH", "USDC"):
                if sym not in addrs:
                    raise Exception(f"Token {sym} not found in SV subgraph")

            # Pair ID = concat of tradedToken address bytes + baseToken address bytes
            wchi = bytes.fromhex(addrs["WCHI"][2:])
            weth = bytes.fromhex(addrs["WETH"][2:])
            usdc = bytes.fromhex(addrs["USDC"][2:])

            self._wchi_weth_pair_id = wchi + weth
            self._weth_usdc_pair_id = weth + usdc

            logging.info("PriceUpdater: Initialised token pair IDs from SV subgraph.")

        except Exception as e:
            logging.error("PriceUpdater: Failed to initialise token pairs: %s", e, exc_info=True)

    def _observation_id(self, pair_id: bytes, virtual_ts: int) -> str:
        """Return the hex observation ID for a pair and virtual timestamp."""
        return "0x" + (pair_id + struct.pack(">q", virtual_ts)).hex()

    def _get_wchi_usdc_for_timestamp(self, ts_int: int):
        """
        Return the WCHI/USDC price for the hour containing ts_int, using the
        SV subgraph PriceObservation data.  Results are cached by hour.

        If there is no observation for that exact hour (e.g. indexer lag),
        we fall back to the most recent earlier observation for each pair.
        Returns None if data is unavailable.
        """
        if self._wchi_weth_pair_id is None or self._weth_usdc_pair_id is None:
            return None

        hour_ts = ts_int - (ts_int % 3600)

        if hour_ts in self._price_cache:
            return self._price_cache[hour_ts]

        wchi_weth_id = self._observation_id(self._wchi_weth_pair_id, hour_ts)
        weth_usdc_id = self._observation_id(self._weth_usdc_pair_id, hour_ts)

        # Query by ID <= target ID, ordered descending.  Because the ID is
        # pair_bytes + timestamp_bytes, the id_lte bound already constrains
        # us to the right pair and hour (or the nearest earlier observation
        # for that pair).  We verify the pair afterwards as a sanity check
        # in case there are no observations at all for a pair yet.
        query = f"""
        {{
          wchiWeth: priceObservations(
            first: 1,
            orderBy: id,
            orderDirection: desc,
            where: {{ id_lte: "{wchi_weth_id}", average24h_not: null }}
          ) {{
            pair {{ id }}
            average24h
          }}
          wethUsdc: priceObservations(
            first: 1,
            orderBy: id,
            orderDirection: desc,
            where: {{ id_lte: "{weth_usdc_id}", average24h_not: null }}
          ) {{
            pair {{ id }}
            average24h
          }}
        }}
        """
        try:
            resp = requests.post(self.sv_subgraph_url, json={"query": query}, timeout=10)
            js = resp.json()
            if "errors" in js:
                raise Exception(f"GraphQL error: {js['errors']}")

            wchi_weth_list = js["data"]["wchiWeth"]
            weth_usdc_list = js["data"]["wethUsdc"]

            wchi_weth_pair_id_hex = "0x" + self._wchi_weth_pair_id.hex()
            weth_usdc_pair_id_hex = "0x" + self._weth_usdc_pair_id.hex()

            if (wchi_weth_list and weth_usdc_list
                    and wchi_weth_list[0]["pair"]["id"] == wchi_weth_pair_id_hex
                    and weth_usdc_list[0]["pair"]["id"] == weth_usdc_pair_id_hex):
                wchi_in_weth = int(wchi_weth_list[0]["average24h"]) / _UQ112
                weth_in_usdc = int(weth_usdc_list[0]["average24h"]) / _UQ112
                price = wchi_in_weth * weth_in_usdc
            else:
                price = None

        except Exception as e:
            logging.error(
                "PriceUpdater: Failed to fetch price observation for ts %d: %s",
                hour_ts, e, exc_info=True,
            )
            price = None

        self._price_cache[hour_ts] = price
        return price

    ##########################################################################
    # Public
    ##########################################################################
    def update_prices(self):
        try:
            self.update_all_trades()
        except Exception as e:
            logging.error("PriceUpdater: Failed to update trades. Error: %s", e, exc_info=True)

    ##########################################################################
    # TRADES
    ##########################################################################
    def update_all_trades(self):
        self._create_svc_trades_table_if_not_exists()
        last_ts = self._get_last_trade_timestamp()

        page_size = 1000
        total_inserted = 0
        while True:
            gql = f"""
            {{
              trades(
                first: {page_size},
                orderBy: timestamp,
                orderDirection: asc,
                where: {{
                  timestamp_gt: {last_ts}
                }}
              ) {{
                id
                timestamp
                buyer
                seller
                amount
                sats
              }}
            }}
            """
            resp = requests.post(self.svc_polygon_subgraph_url, json={"query": gql}, timeout=10)
            js = resp.json()
            if "errors" in js:
                raise Exception(f"GraphQL error: {js['errors']}")

            trades = js["data"]["trades"]
            if not trades:
                break

            inserted, last_ts = self._process_trade_batch(trades, last_ts, page_size)
            total_inserted += inserted
            logging.info(f"Processed batch of {len(trades)} trades (total inserted: {total_inserted})")

            if len(trades) < page_size:
                break

        logging.info(f"Completed trade update. Total trades inserted: {total_inserted}")

    def _process_trade_batch(self, trades, last_ts, page_size):
        """
        Process a batch of trades fetched ordered by timestamp ascending.

        To avoid missing trades that share a timestamp straddling a page
        boundary, we only insert trades whose timestamp is strictly less than
        the last entry's timestamp.  Trades with the last timestamp are left
        for the next outer iteration (which will re-fetch them via
        timestamp_gt=<previous last_ts>).

        If every trade in the batch shares the same timestamp, we can't use
        that strategy, so we fall back to _fetch_and_insert_by_timestamp()
        which pages through that exact timestamp by trade ID.

        Returns (num_inserted, new_last_ts).
        """
        last_batch_ts = int(trades[-1]["timestamp"])

        if int(trades[0]["timestamp"]) == last_batch_ts:
            # Entire batch has the same timestamp — use ID-based sub-paging.
            inserted = self._fetch_and_insert_by_timestamp(last_batch_ts, page_size)
            return inserted, last_batch_ts

        # Normal case: only insert trades strictly before the last timestamp.
        safe_trades = [t for t in trades if int(t["timestamp"]) < last_batch_ts]
        self._insert_svc_trades(safe_trades)
        return len(safe_trades), int(safe_trades[-1]["timestamp"])

    def _fetch_and_insert_by_timestamp(self, ts, page_size):
        """
        Fetch ALL trades for exactly the given timestamp, paging by trade ID.
        Inserts them all and returns the total number inserted.
        """
        last_id = ""
        total_inserted = 0
        while True:
            id_filter = f', id_gt: "{last_id}"' if last_id else ""
            gql = f"""
            {{
              trades(
                first: {page_size},
                orderBy: id,
                orderDirection: asc,
                where: {{
                  timestamp: {ts}{id_filter}
                }}
              ) {{
                id
                timestamp
                buyer
                seller
                amount
                sats
              }}
            }}
            """
            resp = requests.post(self.svc_polygon_subgraph_url, json={"query": gql}, timeout=10)
            js = resp.json()
            if "errors" in js:
                raise Exception(f"GraphQL error: {js['errors']}")

            batch = js["data"]["trades"]
            if not batch:
                break

            self._insert_svc_trades(batch)
            total_inserted += len(batch)
            last_id = batch[-1]["id"]

            if len(batch) < page_size:
                break

        logging.info(f"ID-paged {total_inserted} trade(s) at timestamp {ts}.")
        return total_inserted

    def _create_svc_trades_table_if_not_exists(self):
        sql = f"""
        CREATE TABLE IF NOT EXISTS svc_trades (
            id INT AUTO_INCREMENT PRIMARY KEY,
            trade_id       VARCHAR(255) NOT NULL,
            trade_ts       DATETIME NOT NULL,
            buyer          VARCHAR(255) NOT NULL,
            seller         VARCHAR(255) NOT NULL,
            amount_svc     DOUBLE NOT NULL,
            amount_wchi    DOUBLE NOT NULL,
            volume_svc     DOUBLE NOT NULL,
            volume_wchi    DOUBLE NOT NULL,
            price_usdc     DOUBLE NULL,
            volume_usdc    DOUBLE NULL,
            inserted_at    DATETIME NOT NULL,
            UNIQUE KEY (trade_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, sql)

        alter_price_sql  = "ALTER TABLE svc_trades MODIFY COLUMN `price_usdc` DOUBLE NULL"
        alter_volume_sql = "ALTER TABLE svc_trades MODIFY COLUMN `volume_usdc` DOUBLE NULL"
        self.db.execute_query(self.db.dest_conn, alter_price_sql)
        self.db.execute_query(self.db.dest_conn, alter_volume_sql)

    def _get_last_trade_timestamp(self):
        sql = "SELECT MAX(UNIX_TIMESTAMP(trade_ts)) AS last_ts FROM svc_trades"
        rows = self.db.execute_query(self.db.dest_conn, sql)
        if not rows or rows[0]["last_ts"] is None:
            return 0
        return rows[0]["last_ts"]

    def _insert_svc_trades(self, trades):
        now = datetime.utcnow()

        insert_sql = """
        INSERT IGNORE INTO svc_trades (
            trade_id, trade_ts, buyer, seller,
            amount_svc, amount_wchi, volume_svc, volume_wchi,
            price_usdc, volume_usdc, inserted_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params_list = []
        for t in trades:
            tid      = t["id"]
            ts_int   = int(t["timestamp"])
            trade_dt = datetime.utcfromtimestamp(ts_int)

            buyer  = t["buyer"]
            seller = t["seller"]

            amount_svc  = float(t["amount"]) / 1e4
            amount_wchi = float(t["sats"])   / 1e8
            volume_svc  = amount_svc
            volume_wchi = amount_wchi

            svc_in_wchi = None
            if amount_svc > 0:
                svc_in_wchi = amount_wchi / amount_svc

            wchi_usdc = self._get_wchi_usdc_for_timestamp(ts_int)
            if svc_in_wchi is not None and wchi_usdc is not None:
                price_usdc  = svc_in_wchi * wchi_usdc
                volume_usdc = amount_wchi * wchi_usdc
            else:
                price_usdc  = None
                volume_usdc = None

            params_list.append((
                tid, trade_dt, buyer, seller,
                amount_svc, amount_wchi, volume_svc, volume_wchi,
                price_usdc, volume_usdc, now
            ))

        if params_list:
            self.db.execute_many(self.db.dest_conn, insert_sql, params_list)
            logging.info(f"Inserted {len(params_list)} new trade(s) into svc_trades.")

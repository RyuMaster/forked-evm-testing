# user_updater.py

from .trade_updater import TradeUpdaterBase

class UserUpdater(TradeUpdaterBase):
    def __init__(self, db_manager):
        component_name = 'user_updater'
        key_column = 'user_name'
        dest_table = 'dc_users_trading'
        update_columns = [
            'buy_volume_1_day', 'buy_volume_7_day',
            'sell_volume_1_day', 'sell_volume_7_day',
            'buy_total_volume', 'sell_total_volume',
            'total_volume', 'total_volume_30_day',
            'first_trade_date',
            '10th_trade_date', '100th_trade_date', '1000th_trade_date',
            'biggest_trade', 'last_7days', 'last_30days'  # Added 'last_30days'
        ]
        super().__init__(db_manager, component_name, key_column, dest_table, update_columns)
        
    def get_affected_entities(self, start_height, end_height):
        new_trades_query = """
        SELECT DISTINCT buyer, seller
        FROM share_trade_history
        WHERE height > %s AND height <= %s
        """
        new_trades = self.db.execute_query('source', new_trades_query, (start_height, end_height))
        affected_users = set()
        for trade in new_trades:
            buyer = trade['buyer']
            seller = trade['seller']
            if buyer:
                affected_users.add(buyer)
            if seller:
                affected_users.add(seller)
        return affected_users

    def get_source_query(self, perform_full_update, entity_set):
        if perform_full_update:
            buyer_filter = seller_filter = ""
            params = ()
        elif entity_set:
            placeholders = ','.join(['%s'] * len(entity_set))
            params = tuple(entity_set) * 2  # For buyer and seller
            buyer_filter = f"AND sth.buyer IN ({placeholders})"
            seller_filter = f"AND sth.seller IN ({placeholders})"
        else:
            # No users to update
            return None, ()

        # Generate last_7days and last_30days expressions
        last_7days_expr = self.generate_last_n_days_expr(7)
        last_30days_expr = self.generate_last_n_days_expr(30)

        source_query = f"""
            SELECT
                user_name,
                SUM(buy_volume_1_day) AS buy_volume_1_day,
                SUM(buy_volume_7_day) AS buy_volume_7_day,
                SUM(buy_total_volume) AS buy_total_volume,
                SUM(sell_volume_1_day) AS sell_volume_1_day,
                SUM(sell_volume_7_day) AS sell_volume_7_day,
                SUM(sell_total_volume) AS sell_total_volume,
                SUM(total_volume) AS total_volume,
                SUM(CASE 
                    WHEN first_trade_date >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 30 DAY)) 
                    THEN total_volume 
                    ELSE 0 
                END) AS total_volume_30_day,
                MIN(first_trade_date) AS first_trade_date,
                MAX(biggest_trade) AS biggest_trade,
                MIN(CASE WHEN trade_rank = 10 THEN first_trade_date END) AS `10th_trade_date`,
                MIN(CASE WHEN trade_rank = 100 THEN first_trade_date END) AS `100th_trade_date`,
                MIN(CASE WHEN trade_rank = 1000 THEN first_trade_date END) AS `1000th_trade_date`,
                {last_7days_expr} AS last_7days,
                {last_30days_expr} AS last_30days
            FROM (
                SELECT
                    user_name,
                    CASE WHEN role = 'buyer' AND first_trade_date >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY)) THEN volume ELSE 0 END AS buy_volume_1_day,
                    CASE WHEN role = 'buyer' AND first_trade_date >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY)) THEN volume ELSE 0 END AS buy_volume_7_day,
                    CASE WHEN role = 'buyer' THEN volume ELSE 0 END AS buy_total_volume,
                    CASE WHEN role = 'seller' AND first_trade_date >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY)) THEN volume ELSE 0 END AS sell_volume_1_day,
                    CASE WHEN role = 'seller' AND first_trade_date >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY)) THEN volume ELSE 0 END AS sell_volume_7_day,
                    CASE WHEN role = 'seller' THEN volume ELSE 0 END AS sell_total_volume,
                    volume AS total_volume,
                    first_trade_date,
                    volume AS biggest_trade,
                    ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY first_trade_date) AS trade_rank,
                    DATE(FROM_UNIXTIME(first_trade_date)) AS day,
                    volume AS day_volume
                FROM (
                    SELECT sth.buyer AS user_name, 'buyer' AS role, sth.price * sth.num AS volume, b.date AS first_trade_date
                    FROM share_trade_history sth
                    JOIN blocks b ON sth.height = b.height
                    WHERE sth.buyer IS NOT NULL {buyer_filter}
                    UNION ALL
                    SELECT sth.seller AS user_name, 'seller' AS role, sth.price * sth.num AS volume, b.date AS first_trade_date
                    FROM share_trade_history sth
                    JOIN blocks b ON sth.height = b.height
                    WHERE sth.seller IS NOT NULL {seller_filter}
                ) trades
            ) sub
            GROUP BY user_name
            ORDER BY total_volume_30_day DESC
        """

        return source_query, params

    def generate_last_n_days_expr(self, n):
        expr_list = []
        for i in range(n - 1, -1, -1):
            date_expr = f"DATE_SUB(CURDATE(), INTERVAL {i} DAY)" if i > 0 else "CURDATE()"
            expr = f"IFNULL(SUM(CASE WHEN day = {date_expr} THEN day_volume END), 0)"
            expr_list.append(expr)

        # Function to generate the CONCAT expression with commas between values
        concat_expr = "CONCAT('[', " + ", ', ', ".join(expr_list) + ", ']')"
        return concat_expr

    def get_columns_for_table(self):
        columns = {}
        # Set data type for key_column without 'PRIMARY KEY'
        columns[self.key_column] = 'VARCHAR(255)'

        # Define data types for update_columns
        for col in self.update_columns:
            if col in ['first_trade_date', '10th_trade_date', '100th_trade_date', '1000th_trade_date', 'biggest_trade']:
                columns[col] = 'BIGINT'
            elif col in ['last_7days', 'last_30days']:
                columns[col] = 'LONGTEXT'
            elif 'volume' in col:
                columns[col] = 'BIGINT'
            else:
                columns[col] = 'BIGINT'

        # Add 'updated_at' column
        columns['updated_at'] = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'

        return columns

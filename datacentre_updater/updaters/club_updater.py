# club_updater.py

from .trade_updater import TradeUpdaterBase

class ClubUpdater(TradeUpdaterBase):
    def __init__(self, db_manager):
        component_name = 'club_updater'
        key_column = 'club_id'
        dest_table = 'dc_clubs_trading'
        update_columns = ['last_price', 'volume_1_day', 'volume_7_day', 'last_7days', 'last_7days_price']
        share_type = 'club'  # Set share_type
        super().__init__(db_manager, component_name, key_column, dest_table, update_columns, share_type)
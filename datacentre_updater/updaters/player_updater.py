# player_updater.py

from .trade_updater import TradeUpdaterBase

class PlayerUpdater(TradeUpdaterBase):
    def __init__(self, db_manager):
        component_name = 'player_updater'
        key_column = 'player_id'
        dest_table = 'dc_players_trading'
        update_columns = ['last_price', 'volume_1_day', 'volume_7_day', 'last_7days', 'last_7days_price']
        share_type = 'player'  # Set share_type
        super().__init__(db_manager, component_name, key_column, dest_table, update_columns, share_type)
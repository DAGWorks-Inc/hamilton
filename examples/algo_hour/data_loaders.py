import magic_carpet as mc
import pandas as pd

mcc = mc.MagicCarpetClient()


def spend(demand_db: str, spend_table: str) -> pd.Series:
    """Loads spend_data from spend_location"""
    return mcc.fetch(
        resource=f'{demand_db}.{spend_table}',
        all_partitions_ok=True)['spend']


def signups(demand_db: str, signups_table: str) -> pd.Series:
    """Loads customer acquisition data"""
    return mcc.fetch(
        resource=f'{demand_db}.{signups_table}',
        all_partitions_ok=True)['signups']

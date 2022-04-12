import magic_carpet as mc
import pandas as pd

from hamilton.function_modifiers import extract_columns

mcc = mc.MagicCarpetClient()


@extract_columns(['spend', 'signups'])
def data(demand_db: str, demand_table) -> pd.DataFrame:
    """Loads demand data from hive"""
    return mcc.fetch(
        resource=f'{demand_db}.{demand_table}',
        all_partitions_ok=True)

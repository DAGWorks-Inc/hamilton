import dataclasses
import os
from datetime import datetime
from typing import Any, Collection, Dict, Type

import dataflow
import modin.pandas as pd
import ponder

from hamilton import driver, registry
from hamilton.io.data_adapters import DataSaver
from hamilton.io.materialization import to

ponder.init(os.environ["PONDER_API_KEY"])


def db_connection() -> object:
    import json

    from google.cloud import bigquery
    from google.cloud.bigquery import dbapi
    from google.oauth2 import service_account

    db_con = dbapi.Connection(
        bigquery.Client(
            credentials=service_account.Credentials.from_service_account_info(
                json.loads(open("my_service_account_key.json").read()),
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
        )
    )
    ponder.configure(default_connection=db_con)
    return db_con


@dataclasses.dataclass
class BigQuery(DataSaver):
    table: str
    db_connection: object

    def save_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        data.to_sql(self.table, con=self.db_connection, index=False)
        return {
            "table": self.table,
            "db_type": "bigquery",
            "timestamp": datetime.now().utcnow().timestamp(),
        }

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [pd.DataFrame]

    @classmethod
    def name(cls) -> str:
        return "bigquery"


for adapter in [BigQuery]:
    registry.register_adapter(adapter)

_db_con = object()  # db_connection()

dr = driver.Driver({}, dataflow)
dr.display_all_functions("./dataflow.png")

dr.visualize_materialization(
    to.bigquery(
        dependencies=["final_table"],
        id="final_table_to_bigquery",
        table="lending_club_cleaned",
        db_connection=_db_con,
    ),
    inputs={"db_connection": _db_con, "tablename": "LOANS.ACCEPTED"},
    output_file_path="./dataflow.png",
    render_kwargs={"format": "png"},
)

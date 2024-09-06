import sqlite3

import pipeline
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport

from hamilton import driver
from hamilton.plugins import h_openlineage

# if you don't have a running OpenLineage server, you can use the FileTransport
file_config = FileConfig(
    log_file_path="pipeline.json",
    append=True,
)

# if you have a running OpenLineage server, e.g. marquez, uncomment this line.
# client = OpenLineageClient(url="http://localhost:9000")
client = OpenLineageClient(transport=FileTransport(file_config))

ola = h_openlineage.OpenLineageAdapter(client, "demo_namespace", "my_hamilton_job")

# create inputs to run the DAG
db_client = sqlite3.connect("purchase_data.db")
# create the DAG
dr = driver.Builder().with_modules(pipeline).with_adapters(ola).build()
# display the graph
dr.display_all_functions("graph.png")
# execute & emit lineage
result = dr.execute(
    ["saved_file", "saved_to_db"],
    inputs={
        "db_client": db_client,
        "file_ds_path": "data.csv",
        "file_path": "model.pkl",
        "joined_table_name": "joined_data",
    },
)
# close the DB
db_client.close()

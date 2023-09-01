from datetime import datetime

import click
import uvicorn
from components import aggregations, data_loaders, features, joins, model
from fastapi import FastAPI

from hamilton import base, driver

app = FastAPI()
dr = driver.Driver(
    {"mode": "online"},
    aggregations,
    data_loaders,
    joins,
    features,
    model,
    adapter=base.DefaultAdapter(),
)


@app.get("/predict")
def get_prediction(client_id: int) -> float:
    series_out = dr.execute(
        ["predictions"], inputs={"client_id": client_id, "execution_time": datetime.now()}
    )["predictions"]
    return series_out.values[0]


@click.group()
def cli():
    pass


@cli.command()
@click.option("--port", default=8000, help="Port to serve on")
def serve(port: int):
    """This command will start the server"""
    uvicorn.run(app, host="0.0.0.0", port=port)
    click.echo("Server is running...")


@cli.command()
@click.option("--output-file", default="./out", help="Output file to write to")
def visualize(output_file: str):
    """This command will visualize execution"""
    return dr.visualize_execution(
        ["predictions"], output_file, {}, inputs={"client_id": 0, "execution_time": datetime.now()}
    )


if __name__ == "__main__":
    cli()

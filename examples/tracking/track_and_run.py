import logging
import uuid
from types import ModuleType
from typing import Any, Dict, List

import click
import pandas as pd

import hamilton.driver
from hamilton import base, tracking
from hamilton.tracking import TrackingStateToJSONFile

logging.basicConfig()


def run_and_track(
    modules: List[ModuleType], inputs: Dict[str, Any], config: Dict[str, Any], output_file: str
):
    tracking_state = TrackingStateToJSONFile(str(uuid.uuid4()), output_file)
    adapter = tracking.RunTrackingGraphAdapter(
        result_builder=base.DictResult(), tracking_state=tracking_state
    )
    dr = hamilton.driver.Driver(config, *modules, adapter=adapter)
    dr.execute(final_vars=[var.name for var in dr.list_available_variables()], inputs=inputs)
    tracking_state.finalize()


@click.group()
@click.option("--output-file", required=True, type=click.Path(exists=False))
@click.pass_context
def run(ctx, output_file):
    ctx.ensure_object(dict)
    ctx.obj["output_file"] = output_file


@run.command()
@click.pass_context
def run_hello_world(ctx):
    from examples.hello_world import my_functions

    output_file = ctx.obj["output_file"]
    inputs = {"spend": pd.Series([1, 2, 3, 4, 5]), "signups": pd.Series([1, 2, 3, 4, 5])}
    config = {}
    run_and_track([my_functions], inputs=inputs, config=config, output_file=output_file)


if __name__ == "__main__":
    run()

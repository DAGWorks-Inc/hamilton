import click
import ray_lineage

from hamilton import base, driver
from hamilton.plugins.h_ray import RayGraphAdapter
from hamilton_sdk import adapters


@click.command()
@click.option("--username", required=True, type=str)
@click.option("--project_id", default=1, type=int)
def run(project_id, username):
    try:
        tracker_ray = adapters.HamiltonTracker(
            project_id=project_id,
            username=username,
            dag_name="telemetry_with_ray",
        )
        rga = RayGraphAdapter(result_builder=base.PandasDataFrameResult())
        dr_ray = driver.Builder().with_modules(ray_lineage).with_adapters(rga, tracker_ray).build()
        result_ray = dr_ray.execute(
            final_vars=[
                "node_5s",
                "node_1s_error",
                "add_1_to_previous",
            ]
        )
        print(result_ray)

    except ValueError:
        print("UI should display failure.")
    finally:
        tracker = adapters.HamiltonTracker(
            project_id=project_id,  # modify this as needed
            username=username,
            dag_name="telemetry_without_ray",
        )
        dr_without_ray = driver.Builder().with_modules(ray_lineage).with_adapters(tracker).build()

        result_without_ray = dr_without_ray.execute(final_vars=["node_5s", "add_1_to_previous"])
        print(result_without_ray)


if __name__ == "__main__":
    run()

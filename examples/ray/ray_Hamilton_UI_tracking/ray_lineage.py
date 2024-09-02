import time


def node_5s() -> float:
    start = time.time()
    time.sleep(5)
    return time.time() - start


def add_1_to_previous(node_5s: float) -> float:
    start = time.time()
    time.sleep(1)
    return node_5s + (time.time() - start)


def node_1s_error() -> float:
    time.sleep(1)
    raise ValueError("Does not break telemetry if executed through ray")


if __name__ == "__main__":
    import __main__
    from hamilton import base, driver
    from hamilton.plugins.h_ray import RayGraphAdapter
    from hamilton_sdk import adapters

    username = "admin"

    try:
        tracker_ray = adapters.HamiltonTracker(
            project_id=1,  # modify this as needed
            username=username,
            dag_name="telemetry_with_ray",
        )
        rga = RayGraphAdapter(result_builder=base.PandasDataFrameResult())
        dr_ray = driver.Builder().with_modules(__main__).with_adapters(rga, tracker_ray).build()
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
            project_id=1,  # modify this as needed
            username=username,
            dag_name="telemetry_without_ray",
        )
        dr_without_ray = driver.Builder().with_modules(__main__).with_adapters(tracker).build()

        result_without_ray = dr_without_ray.raw_execute(final_vars=["node_5s", "add_1_to_previous"])
        print(result_without_ray)

import time


def node_5s() -> float:
    print("5s executed")
    start = time.time()
    time.sleep(5)
    return time.time() - start


def at_1_to_previous(node_5s: float) -> float:
    print("1s executed")
    start = time.time()
    time.sleep(10)
    return node_5s + (time.time() - start)


def node_5s_error() -> float:
    print("5s error executed")
    time.sleep(5)
    raise ValueError("Does not break telemetry if executed through ray")


if __name__ == "__main__":
    import ray

    import __main__
    from hamilton import base, driver
    from hamilton.plugins.h_ray import RayGraphAdapter
    from hamilton_sdk import adapters

    username = "jf"

    tracker_ray = adapters.HamiltonTracker(
        project_id=1,  # modify this as needed
        username=username,
        dag_name="ray_telemetry_bug",
    )

    try:
        ray.init()
        rga = RayGraphAdapter(result_builder=base.PandasDataFrameResult())
        dr_ray = driver.Builder().with_modules(__main__).with_adapters(rga, tracker_ray).build()
        result_ray = dr_ray.execute(
            final_vars=[
                "node_5s",
                # 'node_5s_error'
                "at_1_to_previous",
            ]
        )
        print(result_ray)
        ray.shutdown()
        print(result_ray)
    except ValueError:
        print("UI displays no problem")
    # finally:
    #     tracker = adapters.HamiltonTracker(
    #         project_id=1,  # modify this as needed
    #         username=username,
    #         dag_name="telemetry_okay",
    #     )
    #     dr_without_ray = driver.Builder().with_modules(__main__).with_adapters(tracker).build()

    #     result_without_ray = dr_without_ray.execute(final_vars=["node_5s", "node_5s_error"])

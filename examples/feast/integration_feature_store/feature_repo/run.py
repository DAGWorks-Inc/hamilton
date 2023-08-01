import store_definitions
import store_operations

from hamilton import base, driver


def main():
    dr = driver.Driver(
        dict(feast_repository_path=".", feast_config={}),
        store_operations,
        store_definitions,
        adapter=base.SimplePythonGraphAdapter(base.DictResult()),
    )

    final_vars = [
        "apply",
        "registry_diff",
    ]

    inputs = dict(
        trips_stats_3h_path="./data/trips_stats_3h.parquet",
        driver_source_path="./data/driver_stats.parquet",
    )

    out = dr.execute(final_vars=final_vars, inputs=inputs)

    # uncomment these to display execution graph
    # dr.display_all_functions("definitions", {"format": "png"})
    # dr.visualize_execution(final_vars, "exec", {"format": "png"}, inputs=inputs)

    print(out.keys())


if __name__ == "__main__":
    main()

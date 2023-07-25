import aggregate_data
import list_data
import process_data

from hamilton import driver
from hamilton.execution import executors


def main():
    dr = (
        driver.Builder()
        .enable_v2_driver(allow_experimental_mode=True)
        .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=100))
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_modules(aggregate_data, list_data, process_data)
        .build()
    )
    print(dr.execute(final_vars=["statistics_by_city"], inputs={"data_dir": "data"}))


if __name__ == "__main__":
    main()

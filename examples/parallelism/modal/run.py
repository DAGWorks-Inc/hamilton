import modal
import simple_pipeline
from h_modal import ModalExecutor, RemoteExecutionManager

from hamilton import driver


def test_simple_pipeline():
    image = modal.Image.debian_slim().pip_install("sf-hamilton")
    dr = (
        driver.Builder()
        .with_modules(simple_pipeline)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_execution_manager(
            RemoteExecutionManager(
                modal=ModalExecutor(stub_params={}, global_function_params={"image": image})
            )
        )
        .build()
    )
    result = dr.execute(final_vars=["locally_gathered_data"])
    print(result)


if __name__ == "__main__":
    test_simple_pipeline()

# run.py
from hamilton import driver, lifecycle
import hamilton_anthropic

dr = (
    driver.Builder()
    .with_modules(hamilton_anthropic)
    .with_config({"provider": "anthropic"})
    # we just need to add this line to get things printing
    # to the console; see DAGWorks for a more off-the-shelf
    # solution.
    .with_adapters(lifecycle.PrintLn(verbosity=2))
    .build()
)
print(
    dr.execute(
        ["joke_response"],
        inputs={"topic": "ice cream"}
    )
)

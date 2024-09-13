import module_a
import module_b

from hamilton import driver

if __name__ == "__main__":
    dr = (
        driver.Builder()
        .with_modules(
            module_a,
            module_b,
        )
        .allow_module_overrides()
        .build()
    )

    print("builder: ", dr.execute(inputs={}, final_vars=["foo"]))

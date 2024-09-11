import module_a
import module_b
import module_c

from hamilton import driver

if __name__ == "__main__":
    dr = (
        driver.Builder()
        .with_modules(
            module_a,
            module_b,
            module_c,
        )
        .allow_module_overrides()
        .build()
    )

    print("builder: ", dr.execute(inputs={}, final_vars=["foo"]))

    dr2 = driver.Driver({}, *[module_a, module_b], allow_module_overrides=True)
    print("driver: ", dr2.execute(inputs={}, final_vars=["foo"]))

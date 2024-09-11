import module_a

from hamilton import driver

if __name__ == "__main__":
    dr = (
        driver.Builder()
        .with_modules(
            module_a,
            #   module_b
        )
        .build()
    )

    print(dr.execute(inputs={}, final_vars=["foo"]))

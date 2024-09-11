def foo() -> int:
    return 10


def bar(foo: int) -> int:
    return foo + 1


if __name__ == "__main__":
    import __main__ as main
    from hamilton import driver

    dr = driver.Builder().with_modules(main)

    # This produces an error
    dr = dr.with_modules(main)

    dr = dr.build()
    print(dr.execute(inputs={}, final_vars=["bar"]))

from hamilton.function_modifiers import post_pipe, source, step, value


def _post_pipe0(x: str) -> str:
    return x + "-post pipe 0-"


def _post_pipe1(x: str, upstream: str) -> str:
    return x + f"-post pipe 1 with {upstream}-"


def _post_pipe2(x: str, some_val: int) -> str:
    return x + f"-post pipe 2 with value {some_val}-"


def upstream() -> str:
    return "-upstream-"


def user_input() -> str:
    return "-user input-"


@post_pipe(
    step(_post_pipe0),
    step(_post_pipe1, upstream=source("upstream")).named("random"),
    step(_post_pipe2, some_val=value(1000)),
)
def f_of_interest(user_input: str) -> str:
    return user_input + "-raw function-"


def downstream_f(f_of_interest: str) -> str:
    return f_of_interest + "-downstream."


def chain_not_using_post_pipe() -> str:
    t = downstream_f(
        _post_pipe2(_post_pipe1(_post_pipe0(f_of_interest(user_input())), upstream()), 1000)
    )
    return t


if __name__ == "__main__":
    import __main__
    from hamilton import driver

    dr = driver.Builder().with_modules(__main__).build()
    dr.visualize_execution(
        output_file_path="./post_pipe_E2E.png",
        inputs={"user_input": 11},
        final_vars=["downstream_f"],
    )
    print(dr.execute(inputs={"user_input": 11}, final_vars=["downstream_f"]))

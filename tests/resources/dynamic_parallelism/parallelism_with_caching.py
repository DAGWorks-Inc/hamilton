from hamilton import htypes
from hamilton.function_modifiers import cache


def expand_node() -> htypes.Parallelizable[int]:
    for i in (0, 1, 2, 3, 4, 5, 6, 7):
        yield i


@cache(format="json")
def inside_branch(expand_node: int) -> dict:
    return {"value": expand_node}


def collect_node(inside_branch: htypes.Collect[dict]) -> list:
    return list(inside_branch)

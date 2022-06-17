import abc
import dataclasses
from typing import List


@dataclasses.dataclass
class Transform:
    ...


class Import:
    ...


class HamiltonFunction:
    ...


# Commenting out as this is not necessary yet
# class TransformListOptimizer(abc.ABC):
#     @abc.abstractmethod
#     def optimize(self, transforms: List[Transform]) -> List[Transform]:
#         """Optimizes a set of transforms by running equivalence-transformations on it
#
#         @param transforms:
#         @return:
#         """


def render(functions: List[HamiltonFunction], imports: List[Import]) -> str:
    """Renders the hamilton functions to file contents.

    @param functions: Functions to render
    @return: All the rendered functions
    """
    pass


def convert_transforms(transforms: List[Transform]) -> List[HamiltonFunction]:
    """Compiles the transforms into a list of hamilton functions.
    Note that this already optimizes them. We will likely want another
    abstraction here to improve this.

    @param transforms: Transforms to compile to hamilton functions
    @return:
    """
    pass


def gather_transforms(lines: List[str]) -> List[Transform]:
    """Gathers all transformations from a list of lines

    @param contents:
    @return:
    """
    pass


def gather_imports(lines: List[str]) -> List[Import]:
    """Gathers all imports from a file, returning a list of them.

    @param contents:
    @return:
    """


def gather_lines(contents: str) -> List[str]:
    """Gathers all lines in the file. Note that

    @param contents:
    @return:
    """


def transpile_contents(contents: str) -> str:
    """Transpiles contents from dataframe manipulations to a hamilton DAG.
    Heh this whole file could be written using hamilton...

    @param contents: Contents to convert.
    @return: A string representing a hamilton DAG. Should be valid python code.
    """
    lines = gather_lines(contents)
    transforms = gather_transforms(lines)
    imports = gather_imports(lines)
    # TODO -- add optimizer call
    hamilton_functions = convert_transforms(transforms)
    return render(hamilton_functions, imports)

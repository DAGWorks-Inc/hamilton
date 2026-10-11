# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pathlib import Path
from types import ModuleType

from hamilton import driver

CONFIG_HEADER = "HAMILTON_CONFIG"
FINAL_VARS_HEADER = "HAMILTON_FINAL_VARS"
INPUTS_HEADER = "HAMILTON_INPUTS"
OVERRIDES_HEADER = "HAMILTON_OVERRIDES"


def get_git_base_directory() -> str:
    """Get the base path of the current git directory"""
    import subprocess

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print("Error:", result.stderr.strip())
            raise OSError(f"{result.stderr.strip()}")
    except FileNotFoundError as e:
        raise FileNotFoundError("Git command not found. Please make sure Git is installed.") from e


def get_git_reference(git_relative_path: str | Path, git_reference: str) -> str:
    """Get the source code from the specified file and git reference"""
    import subprocess

    try:
        result = subprocess.run(
            ["git", "show", f"{git_reference}:{git_relative_path}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if result.returncode == 0:
            return result.stdout.strip()
        elif result.returncode == 128:
            # TODO catch the following error:
            # fatal: Path '{git_relative_path}' exists on disk, but not in '{git_revision}'.
            return
        else:
            return
    except FileNotFoundError as e:
        raise FileNotFoundError("Git command not found. Please make sure Git is installed.") from e


def version_hamilton_functions(module: ModuleType) -> dict[str, str]:
    """Hash the source code of Hamilton functions from a module"""
    from hamilton import graph_types, graph_utils

    origins_version: dict[str, str] = dict()

    for origin_name, _ in graph_utils.find_functions(module):
        origin_callable = getattr(module, origin_name)
        origins_version[origin_name] = graph_types.hash_source_code(origin_callable, strip=True)

    return origins_version


def hash_hamilton_nodes(dr: driver.Driver) -> dict[str, str]:
    """Hash the source code of Hamilton functions from nodes in a Driver"""
    from hamilton import graph_types

    graph = graph_types.HamiltonGraph.from_graph(dr.graph)
    return {n.name: n.version for n in graph.nodes}


def map_nodes_to_functions(dr: driver.Driver) -> dict[str, str]:
    """Get a mapping from node name to Hamilton function name"""
    from hamilton import graph_types

    graph = graph_types.HamiltonGraph.from_graph(dr.graph)

    node_to_function = dict()
    for n in graph.nodes:
        # is None for config nodes
        if n.originating_functions is None:
            continue

        node_callable = n.originating_functions[0]
        node_to_function[n.name] = node_callable.__name__

    return node_to_function


def hash_dataflow(nodes_version: dict[str, str]) -> str:
    """Create a dataflow hash from the hashes of its nodes"""
    import hashlib

    sorted_nodes = sorted(nodes_version.values())
    return hashlib.sha256(str(sorted_nodes).encode()).hexdigest()


def load_modules_from_git(
    module_paths: list[Path], git_reference: str = "HEAD"
) -> list[ModuleType]:
    """Dynamically import modules for a git reference"""
    from hamilton import ad_hoc_utils

    git_base_dir = Path(get_git_base_directory())

    modules = []
    for module_path in module_paths:
        relative_to_git = module_path.relative_to(git_base_dir)
        previous_revision = get_git_reference(relative_to_git, git_reference)
        module = ad_hoc_utils.module_from_source(previous_revision)
        modules.append(module)

    return modules


def diff_nodes_against_functions(
    nodes_version: dict[str, str],
    origins_version: dict[str, str],
    node_to_origin: dict[str, str],
) -> dict:
    """Compare the nodes version from a built Driver to
    the origins version from module source code when a second
    Driver can't be instantiated

    :nodes_version: mapping from node name to its origin function hash
    :origins_version: mapping from origin function name to origin function hash
    :node_to_origin: mapping from node name to origin name
    """
    node_added = []
    code_diff = []
    origins_removed = []

    # iterate over nodes from built Driver
    for node_name, node_version in nodes_version.items():
        origin_name = node_to_origin[node_name]
        origin_version = origins_version.get(origin_name)

        # if origin_version is None, the node is new
        if origin_version is None:
            node_added.append(node_name)
            continue

        # if node_version != origin_version, the current node's origin
        # function existed before, but was edited since origin_version
        if node_version != origin_version:
            code_diff.append(node_name)

    nodes_origin_version = set(nodes_version.values())
    # iterate over origin functions from source code
    for origin_name, origin_version in origins_version.items():
        # if origin_version not found in nodes_version, the
        # origin function is not used by the built Driver
        if origin_version not in nodes_origin_version:
            origins_removed.append(origin_name)

    return dict(
        nodes_added=node_added,
        code_diff=code_diff,
        origins_removed=origins_removed,
    )


def diff_versions(current_map: dict[str, str], reference_map: dict[str, str]) -> dict:
    """Generic diff of two {name: hash} mappings (can be node or origin name)

    :mapping_v1: mapping from node (or function) name to its function hash
    :mapping_v2: mapping from node (or function) name to its function hash
    """
    current_only, reference_only, edit = [], [], []

    for node_name, v1 in current_map.items():
        v2 = reference_map.get(node_name)
        if v2 is None:
            current_only.append(node_name)
            continue

        if v1 != v2:
            edit.append(node_name)

    for node_name, _ in reference_map.items():
        v1 = current_map.get(node_name)
        if v1 is None:
            reference_only.append(node_name)
            continue

    return dict(current_only=current_only, reference_only=reference_only, edit=edit)


def _custom_diff_style(
    *,
    node,
    node_class,
    current_only: list[str],
    reference_only: list[str],
    edit: list[str],
):
    """Custom visualization style for the diff of 2 dataflows"""
    if node.name in current_only:
        style = ({"fillcolor": "greenyellow"}, node_class, "Current only")

    elif node.name in reference_only:
        style = ({"fillcolor": "tomato1"}, node_class, "Reference only")

    elif node.name in edit:
        style = ({"fillcolor": "yellow2"}, node_class, "Edited")

    else:
        style = ({}, node_class, None)

    return style


def visualize_diff(
    current_dr: driver.Driver,
    reference_dr: driver.Driver,
    current_only: list[str],
    reference_only: list[str],
    edit: list[str],
):
    """Visualize the diff of 2 dataflows.

    Uses the union of sets of nodes from driver 1 and driver 2.
    """
    import functools

    from hamilton import graph

    all_nodes = set(reference_dr.graph.get_nodes()).union(set(current_dr.graph.get_nodes()))

    diff_style = functools.partial(
        _custom_diff_style,
        current_only=current_only,
        reference_only=reference_only,
        edit=edit,
    )

    return graph.create_graphviz_graph(
        nodes=all_nodes,
        comment="Diff viz",
        graphviz_kwargs=dict(),
        custom_style_function=diff_style,
        node_modifiers=dict(),
        strictly_display_only_nodes_passed_in=True,
    )


# TODO refactor ContextLoader to a class
# TODO support loading from pyproject.toml
def load_context(file_path: Path) -> dict:
    if not file_path.exists():
        raise FileNotFoundError(f"`{file_path}` doesn't exist.")

    extension = file_path.suffix
    if extension == ".json":
        context = _read_json_context(file_path)
    elif extension == ".py":
        context = _read_py_context(file_path)
    else:
        raise ValueError(f"Received extension `{extension}` is unsupported.")

    context = _validate_context(context)
    return context


def _validate_context(context: dict) -> dict:
    if context[CONFIG_HEADER] is None:
        context[CONFIG_HEADER] = {}

    if context[FINAL_VARS_HEADER] is None:
        context[FINAL_VARS_HEADER] = []

    if context[INPUTS_HEADER] is None:
        context[INPUTS_HEADER] = {}

    if context[OVERRIDES_HEADER] is None:
        context[OVERRIDES_HEADER] = {}

    return context


def _read_json_context(file_path: Path) -> dict:
    """"""
    import json

    data = json.load(file_path.open())

    context = {}
    for k in [
        CONFIG_HEADER,
        FINAL_VARS_HEADER,
        INPUTS_HEADER,
        OVERRIDES_HEADER,
    ]:
        context[k] = data.get(k, None)

    return context


def _read_py_context(file_path: Path) -> dict:
    import importlib

    spec = importlib.util.spec_from_file_location("cli_config", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    context = {}
    for k in [
        CONFIG_HEADER,
        FINAL_VARS_HEADER,
        INPUTS_HEADER,
        OVERRIDES_HEADER,
    ]:
        context[k] = getattr(module, k, None)

    return context

def _has_existing_test(test_file_path: Path, test_func_name: str) -> bool:
    """Check via AST whether the test file already declares test_func_name."""
    import ast

    if not test_file_path.exists():
        return False
    try:
        tree = ast.parse(test_file_path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == test_func_name:
                return True
    except Exception:
        return False
    return False


def generate_test_stub_for_function(func_name: str, func: object, module_name: str) -> str:
    """Generate the pytest-style unit test stub for a single Hamilton function."""
    import inspect

    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    param_lines = []
    call_args = []
    has_pandas = False

    for param in params:
        annotation_str = ""
        if param.annotation != inspect.Parameter.empty:
            if hasattr(param.annotation, "__name__"):
                annotation_str = param.annotation.__name__
            else:
                annotation_str = str(param.annotation).replace("typing.", "")

        if "Series" in annotation_str or "DataFrame" in annotation_str:
            has_pandas = True
            default_val = "pd.Series([])" if "Series" in annotation_str else "pd.DataFrame({})"
            param_lines.append(f"    {param.name}: {annotation_str} = {default_val}  # TODO: fill in")
        elif annotation_str:
            param_lines.append(f"    {param.name}: {annotation_str} = None  # TODO: fill in")
        else:
            param_lines.append(f"    {param.name} = None  # TODO: fill in")
        call_args.append(param.name)

    call_expr = f"{module_name}.{func_name}({', '.join(call_args)})"

    if has_pandas:
        assertion_block = (
            "    # TODO: fill in expected\n"
            "    expected = None\n"
            "    pd.testing.assert_series_equal(actual, expected)"
        )
    else:
        assertion_block = (
            "    # TODO: fill in expected\n"
            "    expected = None\n"
            "    assert actual == expected"
        )

    params_code = "\n".join(param_lines)
    if params_code:
        params_code += "\n"

    return f"""def test_{func_name}():
{params_code}    actual = {call_expr}
{assertion_block}
"""


def create_test_stubs(module_path: Path, output_file: Path | None = None) -> Path:
    """Scan a module file and generate unit test stubs for its Hamilton functions."""
    from hamilton import ad_hoc_utils, graph_utils

    module_name = module_path.stem
    if output_file is None:
        output_file = module_path.parent / f"test_{module_name}.py"

    mod = ad_hoc_utils.module_from_source(module_path.read_text())
    functions = graph_utils.find_functions(mod)

    new_stubs = []
    for func_name, func_obj in functions:
        test_func_name = f"test_{func_name}"
        if not _has_existing_test(output_file, test_func_name):
            new_stubs.append(generate_test_stub_for_function(func_name, func_obj, module_name))

    if not new_stubs:
        return output_file

    if output_file.exists():
        existing_text = output_file.read_text()
        spacing = "\n\n" if not existing_text.endswith("\n\n") else ""
        output_file.write_text(existing_text + spacing + "\n\n".join(new_stubs) + "\n")
    else:
        needs_pandas = any("pd." in stub for stub in new_stubs)
        header_lines = ["import pytest"]
        if needs_pandas:
            header_lines.append("import pandas as pd")
        header_lines.append(f"import {module_name}\n\n")
        header = "\n".join(header_lines)
        output_file.write_text(header + "\n\n".join(new_stubs) + "\n")

    return output_file

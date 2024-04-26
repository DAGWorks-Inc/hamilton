"""
Module for use with jupyter notebooks to facilitate Hamilton development.

Usage:
   > # To load it
   > %load_ext hamilton.plugins.jupyter_magic

   > %%cell_to_module -m MODULE_NAME
   > def my_hamilton_funcs(): ...

If you are developing on this module you'll then want to use:

    > %reload_ext hamilton.plugins.jupyter_magic

"""

import ast
import graphlib
import importlib
import os
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Literal, Optional, Set, Union

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.core.shellapp import InteractiveShellApp
from IPython.display import HTML, display

from hamilton import ad_hoc_utils, driver
from hamilton.lifecycle import base as lifecycle_base


def get_assigned_variables(module_node: ast.Module) -> Set[str]:
    """Get the set of variable names assigned in a AST Module"""
    assigned_vars = set()

    def visit_node(ast_node):
        if isinstance(ast_node, ast.Assign):
            for target in ast_node.targets:
                if isinstance(target, ast.Name):
                    assigned_vars.add(target.id)

        for child_node in ast.iter_child_nodes(ast_node):
            visit_node(child_node)

    visit_node(module_node)
    return assigned_vars


def get_function_assigned_variables(code):
    assigned_variables = []

    def visit_Assign(node):
        for target in node.targets:
            if isinstance(target, ast.Name):
                assigned_variables.append(target.id)

    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for sub_node in ast.walk(node):
                if isinstance(sub_node, ast.Assign):
                    visit_Assign(sub_node)

    return assigned_variables


def execute_and_get_assigned_values(shell: InteractiveShellApp, cell: str) -> Dict[str, Any]:
    """Execute source code from a cell in the user namespace and collect
    the values of all assigned variables into a dictionary.
    """
    shell.ex(cell)
    expr = shell.input_transformer_manager.transform_cell(cell)
    expr_ast = shell.compile.ast_parse(expr)
    return {name: shell.user_ns[name] for name in get_assigned_variables(expr_ast)}


def topologically_sorted_nodes(nodes):
    graph = {n.name: set([*n.required_dependencies, *n.optional_dependencies]) for n in nodes}
    sorter = graphlib.TopologicalSorter(graph)
    return list(sorter.static_order())


def rebuild_driver(
    dr: Optional[driver.Driver] = None,
    config: Optional[Dict[str, Any]] = None,
    modules: Optional[List[ModuleType]] = None,
    adapters: Optional[
        Union[lifecycle_base.LifecycleAdapter, List[lifecycle_base.LifecycleAdapter]]
    ] = None,
    graph_executor: Optional[driver.GraphExecutor] = None,
    reload_modules: Literal[True, False, "strict"] = False,
) -> driver.Driver:
    _driver = dr if dr else driver.Builder().build()
    _config = config if config else _driver.graph.config
    _modules = modules if modules else _driver.graph_modules
    _adapter = adapters if adapters else _driver.adapter
    _graph_executor = graph_executor if graph_executor else _driver.graph_executor

    if reload_modules:
        new_modules = []
        for m in _modules:
            try:
                new_module = importlib.reload(m)
            except ImportError as e:
                if reload_modules == "strict":
                    raise e
                new_module = m
            new_modules.append(new_module)
        _modules = new_modules

    return driver.Driver(
        _config,
        *_modules,
        adapter=_adapter,
        _graph_executor=_graph_executor,
        _use_legacy_adapter=False,
    )


def normalize_result_names(node_name: str) -> str:
    return node_name.replace(".", "__")


def display_in_databricks(dot):
    try:
        display(HTML(dot.pipe(format="svg").decode("utf-8")))
    except Exception as e:
        print(
            f"Failed to display graph: {e}\n"
            "Please ensure graphviz is installed via `%sh apt install -y graphviz`"
        )
        return
    return dot


def insert_cell_with_content():
    """Calling this function before .set_next_input() will output text content to
    the next code cell.

    This works by printing JavaScript in the current cell's HTML output

    adapted from: https://stackoverflow.com/questions/65379879/define-a-ipython-magic-which-replaces-the-content-of-the-next-cell
    """
    js_script = r"""<script>

        if (document.getElementById('notebook-container')) {
            //console.log('Jupyter Notebook');
            allCells = document.getElementById('notebook-container').children;
            selectionClass = /\bselected\b/;
            jupyter = 'notebook';
        }
        else if (document.getElementsByClassName('jp-Notebook-cell').length){
            //console.log('Jupyter Lab');
            allCells = document.getElementsByClassName('jp-Notebook-cell');
            selectionClass = /\bjp-mod-selected\b/;
            jupyter = 'lab';
        }
        else {
            console.log('Unknown Environment');
        }

        if (typeof allCells !== 'undefined') {
            for (i = 0; i < allCells.length - 1; i++) {
                if(selectionClass.test(allCells[i].getAttribute('class'))){
                    allCells[i + 1].remove();

                    // remove output indicators of current cell
                    window.setTimeout(function(){
                        if(jupyter === 'lab') {
                            allCells[i].setAttribute('class', allCells[i].getAttribute('class') + ' jp-mod-noOutputs');
                            allCells[i].getElementsByClassName('jp-OutputArea jp-Cell-outputArea')[0].innerHTML = '';
                        } else if(jupyter === 'notebook'){
                            allCells[i].getElementsByClassName('output')[0].innerHTML = '';
                        }
                    }, 20);

                    break;
                }
            }
        }
        </script>"""

    # remove next cell
    display(HTML(js_script))


def determine_notebook_type() -> str:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return "databricks"
    return "default"


@magics_class
class HamiltonMagics(Magics):
    """Magics to facilitate interactive Hamilton development in notebooks."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver = None
        self.driver_name = None
        self.builder = None
        self.external_modules = list()
        self.final_vars = list()  # empty list will be converted to "all variables"

        if not hasattr(self, "notebook_env"):
            self.notebook_env = determine_notebook_type()

    @magic_arguments()  # needed on top to enable parsing
    @argument("module_name", help="Name for the module defined in this cell.")
    @argument(
        "-d",
        "--display",
        nargs="?",
        const=True,
        help="Display the dataflow. The argument is the variable name of a dictionary of visualization kwargs; else {}.",
    )
    @argument(
        "-x",
        "--execute",
        nargs="?",
        const=True,
        help="Execute the dataflow. The argument is the variable name of a list of nodes; else execute all nodes.",
    )
    @argument(
        "-b",
        "--builder",
        help="Builder to which the module will be added and used for execution. Allows to pass Config and Adapters",
    )
    @argument(
        "-i",
        "--inputs",
        help="Execution inputs. The argument is the variable name of a dict of inputs; else {}.",
    )
    @argument(
        "-o",
        "--overrides",
        help="Execution overrides. The argument is the variable name of a dict of overrides; else {}.",
    )
    @argument(
        "-h",
        "--hide_results",
        action="store_true",
        help="Hides the automatic display of execution results. ",
    )
    @argument(
        "-w",
        "--write_to_file",
        nargs="?",
        const=True,
        help="Write cell content to a file. The argument is the file path; else write to {module_name}.py",
    )
    @cell_magic
    def cell_to_module(self, line, cell):
        """Turn a notebook cell into a Hamilton module definition. This allows you to define
        and execute a dataflow from a single cell.

        For example:
        ```
        %%cell_to_module dataflow --display --execute
        def A(external_input: int) -> int:
          return external_input ** 3

        def B(A: int) -> bool:
          return (A % 3) > 2
        ```
        """
        # shell.ex() is equivalent to exec(), but in the user namespace (i.e. notebook context).
        # This allows imports and functions defined in the magic cell %%cell_to_module to be
        # directly accessed from the notebook
        self.shell.ex(cell)

        args = parse_argstring(self.cell_to_module, line)  # specify how to parse by passing method
        module_name = args.module_name
        builder = self.shell.user_ns[args.builder] if args.builder else driver.Builder()
        inputs = self.shell.user_ns[args.inputs] if args.inputs else {}
        overrides = self.shell.user_ns[args.overrides] if args.overrides else {}
        display_config = (
            self.shell.user_ns[args.display] if args.display not in [True, None] else {}
        )

        if args.write_to_file:
            if isinstance(args.write_to_file, str):
                file_path = Path(args.write_to_file)
            else:
                file_path = Path(f"{module_name}.py")
            file_path.write_text(cell)

        # create modules and build driver
        module_object = ad_hoc_utils.module_from_source(cell, module_name)
        self.shell.push({module_name: module_object})
        base_dr = builder.build()  # easier to rebuild a Driver than messing with Builder
        dr = rebuild_driver(dr=base_dr, modules=[*base_dr.graph_modules, module_object])

        # determine final vars
        if args.execute not in [True, None]:
            final_vars = self.shell.user_ns[args.execute]
        else:
            nodes = [n for n in dr.list_available_variables() if not n.is_external_input]
            final_vars = topologically_sorted_nodes(nodes)

        # visualize
        if args.display:
            # try/except for display config or invalid `.visualize_execution()` inputs
            try:
                if args.execute:
                    dot = dr.visualize_execution(
                        final_vars=final_vars,
                        inputs=inputs,
                        overrides=overrides,
                        **display_config,
                    )
                else:
                    dot = dr.display_all_functions(**display_config)
            except Exception as e:
                print(f"The display config: {display_config}\n\n" f"Failed with exception {e}")
                dot = dr.display_all_functions()

            if self.notebook_env == "databricks":
                display_in_databricks(dot)
            else:
                display(dot)

        # execute
        if args.execute:
            results = dr.execute(
                final_vars=final_vars,
                inputs=inputs,
                overrides=overrides,
            )
            # normalize variable names that contain a `.` character like @pipe(step())
            results = {normalize_result_names(name): value for name, value in results.items()}
            self.shell.push(results)

            if args.hide_results:
                return
            # results will follow the order of `final_vars` or topologically sorted if all vars
            display(*(results[n] for n in final_vars))

    @magic_arguments()
    @argument("name", type=str, help="Creates a dictionary fromt the cell's content.")
    @cell_magic
    def set_dict(self, line: str, cell: str):
        """Execute the cell and store all assigned variables as inputs"""
        args = parse_argstring(self.set_dict, line)
        self.shell.user_ns[args.name] = execute_and_get_assigned_values(self.shell, cell)

    @line_magic
    def module_to_cell(self, line):
        """Insert in the next cell the source code from the module (.py)
        at the path specified by `line`.

        This is useful if you have an existing Hamilton module and want to pull in the contents
        for further development in the notebook.
        """

        # JavaScript magic to generate a new cell
        insert_cell_with_content()

        module_path = Path(line)
        # insert our custom %%with_functions magic at the top of the cell
        header = f"%%cell_to_module {module_path.stem}\n"
        module_source = module_path.read_text()
        # insert source code as text in the next cell
        self.shell.set_next_input(header + module_source, replace=False)


def load_ipython_extension(ipython: InteractiveShellApp):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    ipython.register_magics(HamiltonMagics)

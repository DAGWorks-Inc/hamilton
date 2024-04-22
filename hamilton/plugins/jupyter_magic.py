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
    """Magics to facilitate interactive Hamilton development in notebooks.

    Use `%%cell_to_module` to define a Python module in a single cell. A Hamilton Driver
    is automatically built using its content.

    Use `%%set_driver` to explicitly define a Driver, allowing you to:
    - pass a Driver config
    - pass external Python modules
    - pass Adapters
    - pass Executors
    Then, `%%cell_to_module` will automatically add its module to this Driver.

    Use `%%set_inputs`, `%%set_overrides`, and `%%set_final_vars` to customize execution.
    These values will be used when `%%cell_to_module` receives the `--execute` flag.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver = None
        self.driver_name = None
        self.external_modules = list()
        self.inputs = dict()
        self.overrides = dict()
        self.final_vars = list()  # empty list will be converted to "all variables"
        self.display_config = dict()

        if not hasattr(self, "notebook_env"):
            self.notebook_env = determine_notebook_type()

    @magic_arguments()  # needed on top to enable parsing
    @argument("-m", "--module_name", help="Module name to provide. Default is jupyter_module.")
    @argument(
        "-x",
        "--execute",
        action="store_true",
        help="Flag to execute the dataflow using the Driver, final_vars, inputs, and overrides.",
    )
    @argument(
        "-w",
        "--write_to_file",
        action="store_true",
        help="Flag to write the cell to a Python file named according to {module_name}.py",
    )
    @argument(
        "-d",
        "--display",
        action="store_true",
        help="Flag to display all functions or the execution path (if --execute is present).",
    )
    @cell_magic
    def cell_to_module(self, line, cell):
        """Execute the cell and dynamically create a Python module from its content.
        A Hamilton Driver is automatically instantiated with that module for variable `{MODULE_NAME}_dr`.

        > %%cell_to_module -m MODULE_NAME --display --rebuild-drivers
        """
        # shell.ex() is equivalent to exec(), but in the user namespace (i.e. notebook context).
        # This allows imports and functions defined in the magic cell %%cell_to_module to be
        # directly accessed from the notebook
        self.shell.ex(cell)

        args = parse_argstring(self.cell_to_module, line)  # specify how to parse by passing method
        module_name = "jupyter_module" if args.module_name is None else args.module_name
        if args.write_to_file:
            with open(f"{module_name}.py", "w") as f:
                f.write(cell)

        # create a module from the cell and push it to user namespace
        module_object = ad_hoc_utils.module_from_source(cell)
        self.shell.push({module_name: module_object})

        # update the Driver, keeps existing modules, config, adapter; update it in user ns
        self.driver = rebuild_driver(
            self.driver,
            modules=[module_object] + self.external_modules,
        )
        self.shell.push({self.driver_name: self.driver})

        # we don't assign the full list to self.final_vars, otherwise the next iteration
        # won't pickup on newly available nodes
        if self.final_vars == []:
            final_vars = [
                n.name for n in self.driver.list_available_variables() if not n.is_external_input
            ]
        else:
            final_vars = self.final_vars

        try:
            if args.execute:
                dot = self.driver.visualize_execution(
                    final_vars=final_vars,
                    inputs=self.inputs,
                    overrides=self.overrides,
                    **self.display_config,
                )
            else:
                dot = self.driver.display_all_functions(**self.display_config)
        except Exception as e:
            print(f"The display config: {self.display_config}\n\n" f"Failed with exception {e}")
            self.driver.display_all_functions()

        if self.notebook_env == "databricks":
            display_in_databricks(dot)
        else:
            display(dot)

        if args.execute:
            results = self.driver.execute(
                final_vars=final_vars,
                inputs=self.inputs,
                overrides=self.overrides,
            )
            results = {normalize_result_names(name): value for name, value in results.items()}
            self.shell.push(results)

    @cell_magic
    def set_driver(self, line: str, cell: str):
        """Execute the cell and stores the defined Driver.

        The cell can contain variable assignment, but only the first Driver
        defined will be stored.
        """
        assigned_values = execute_and_get_assigned_values(self.shell, cell)

        for name, value in assigned_values.items():
            if isinstance(value, driver.Driver):
                self.driver = value
                self.driver_name = name
                self.external_modules = list(value.graph_modules)
                return

        raise ValueError("No Driver variable found in cell.")

    @magic_arguments()
    @argument(
        "--all",
        action="store_true",
        help="Flag to include all variables and ignore the cell content.",
    )
    @cell_magic
    def set_final_vars(self, line: str, cell: str):
        """Execute the cell and stores the list of strings passed.

        The cell should contain a single variable assignment, which is a list of strings.
        """
        args = parse_argstring(self.set_final_vars, line)
        if args.all:
            self.final_vars = []
            return

        assigned_values = execute_and_get_assigned_values(self.shell, cell)

        if len(assigned_values) != 1:
            raise ValueError("The cell should contain a single variable that's a list of strings.")

        key, value = next(iter(assigned_values.items()))
        if not isinstance(assigned_values[key], list):
            raise TypeError(f"The variable `{key}` isn't a list.")

        self.final_vars = value

    @cell_magic
    def set_inputs(self, line: str, cell: str):
        """Execute the cell and store all assigned variables as inputs"""
        self.inputs = execute_and_get_assigned_values(self.shell, cell)

    @cell_magic
    def set_overrides(self, line: str, cell: str):
        """Execute the cell and store all assigned variables as overrides"""
        self.overrides = execute_and_get_assigned_values(self.shell, cell)

    @cell_magic
    def set_display_config(self, line: str, cell: str):
        self.display_config = execute_and_get_assigned_values(self.shell, cell)

    @line_magic
    def insert_module(self, line):
        """Insert in the next cell the source code from the module (.py)
        at the path specified by `line`.

        This is useful if you have an existing Hamilton module and want to pull in the contents
        for further development in the notebook.
        """

        # JavaScript magic to generate a new cell
        insert_cell_with_content()

        module_path = Path(line)
        # insert our custom %%with_functions magic at the top of the cell
        header = f"%%cell_to_module -m {module_path.stem}\n\n"
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

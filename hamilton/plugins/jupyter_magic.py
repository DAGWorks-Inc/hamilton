import argparse
import ast
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.core.shellapp import InteractiveShellApp
from IPython.display import HTML, display
from IPython.utils.process import arg_split

from hamilton import ad_hoc_utils, driver


def get_assigned_variables(module_node: ast.Module) -> Set[str]:
    """Get the set of variable names assigned in a AST Module"""
    assigned_vars = set()

    def visit_node(ast_node):
        """Recursive function looking for assigned variable names"""
        if isinstance(ast_node, ast.Assign):
            for target in ast_node.targets:
                if isinstance(target, ast.Name):
                    assigned_vars.add(target.id)

        for child_node in ast.iter_child_nodes(ast_node):
            visit_node(child_node)

    visit_node(module_node)
    return assigned_vars


def execute_and_get_assigned_values(shell: InteractiveShellApp, cell: str) -> Dict[str, Any]:
    """Execute source code from a cell in the user namespace and collect
    the values of all assigned variables into a dictionary.
    """
    shell.ex(cell)
    expr = shell.input_transformer_manager.transform_cell(cell)
    expr_ast = shell.compile.ast_parse(expr)
    return {name: shell.user_ns[name] for name in get_assigned_variables(expr_ast)}


def topological_sort(nodes):
    """Sort the nodes for nice output display"""

    def dfs(node, visited, stack):
        visited.add(node)
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                dfs(neighbor, visited, stack)
        stack.append(node)

    graph = {n.name: set([*n.required_dependencies, *n.optional_dependencies]) for n in nodes}
    visited = set()
    stack = []

    for node in graph:
        if node not in visited:
            dfs(node, visited, stack)

    return stack


def _normalize_result_names(node_name: str) -> str:
    """Remove periods from the name of dynamically generated Hamilton nodes"""
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


def parse_known_argstring(magic_func, argstring) -> Tuple[argparse.Namespace, List[str]]:
    """IPython magic arguments parsing doesn't allow unknown args.
    Used instead of IPython.core.magic_arguments.parse_argstring

    IPython ref: https://github.com/ipython/ipython/blob/43781b39a67f02ff4e9ae63484387f654dd045d4/IPython/core/magic_arguments.py#L164
    argparse ref: https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_known_args
    """
    argv = arg_split(argstring)
    # magic_func.parser is an argparse.ArgumentParser subclass
    known, unknown = magic_func.parser.parse_known_args(argv)
    return known, unknown


def parse_config(config_string):
    config = {}
    for item in config_string.split():
        key, value = item.split("=")
        config[key] = value
    return config


@magics_class
class HamiltonMagics(Magics):
    """Magics to facilitate interactive Hamilton development in notebooks."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.builder = None
        self.notebook_env = determine_notebook_type()

    def resolve_unknown_args_cell_to_module(self, unknown: List[str]):
        """Handle unknown arguments. It won't make the magic execution fail."""

        # deprecated in V2 because it's less useful since `%%cell_to_module` can execute itself
        if any(arg in ("-r", "--rebuilder-drivers") for arg in unknown):
            print(
                "DeprecationWarning: -r/--rebuilder-drivers no long does anything and will be removed in future releases."
            )

        # deprecated in V2 because it relates to the deprecated -r/--rebuild-drivers
        if any(arg in ("-v", "--verbose") for arg in unknown):
            print(
                "DeprecationWarning: -v/--verbose no long does anything and will be removed in future releases."
            )

        # there for backwards compatibility. Equivalent to calling `%%cell_to_module?`
        # not included as @argument because it's not really a function arg to %%cell_to_module
        if any(arg in ("-h", "--help") for arg in unknown):
            print(help(self.cell_to_module))

    @magic_arguments()  # needed on top to enable parsing
    @argument("module_name", nargs="?", help="Name for the module defined in this cell.")
    @argument(
        "-m",
        "--module_name",
        nargs="?",
        const=True,
        help="Alias for positional argument `module_name`. There for backwards compatibility. Prefer the position arg.",
    )
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
        "-c",
        "--config",
        help="Config to build a Driver. Passing -c/--config at the same time as a Builder -b/--builder with a config will raise an exception.",
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
        "--hide_results",
        action="store_true",
        help="Hides the automatic display of execution results.",
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
        def A() -> int:
          return 37

        def B(A: int) -> bool:
          return (A % 3) > 2
        ```
        """
        # shell.ex() is equivalent to exec(), but in the user namespace (i.e. notebook context).
        # This allows imports and functions defined in the magic cell %%cell_to_module to be
        # directly accessed from the notebook
        self.shell.ex(cell)

        args, unknown_args = parse_known_argstring(
            self.cell_to_module, line
        )  # specify how to parse by passing method
        self.resolve_unknown_args_cell_to_module(unknown_args)

        # validate variables exist in the user namespace expect `config` because it's a special case
        # will exit using `return` in case of error
        args_that_read_user_namespace = ["display", "builder", "inputs", "overrides"]
        for name, value in vars(args).items():
            if name not in args_that_read_user_namespace:
                continue

            # special case: `display` can be passed as a flag (=True), without a config
            if name == "display" and value is True:
                continue

            # main case: exit if variable is not in user namespace
            if value and self.shell.user_ns.get(value) is None:
                return f"KeyError: Received `--{name} {value}` but variable not found."

        # special case: `config` expects potentially a JSON string
        # there for backwards compatibility

        # default case: didn't receive `-c/--config`. Set an empty dict
        if args.config is None:
            config = {}
        # case 1, 2, 3: `-c/--config` is specified
        # case 1: -c/--config refers to variable in the user namespace
        elif self.shell.user_ns.get(args.config):
            config = self.shell.user_ns.get(args.config)
        # case 2: parse using key=value
        elif "=" in args.config:
            config = parse_config(args.config)
        # case 3: parse as JSON
        elif ":" in args.config:
            try:
                # strip quotation marks added by IPython and avoid mutating `args`
                config_str = args.config.strip("'\"")
                config = json.loads(config_str)
            except json.JSONDecodeError:
                print(f"JSONDecodeError: Failed to parse `config` as JSON. Received {value}")
                return

        # get the default values of args
        module_name = args.module_name
        base_builder = self.shell.user_ns[args.builder] if args.builder else driver.Builder()
        inputs = self.shell.user_ns[args.inputs] if args.inputs else {}
        overrides = self.shell.user_ns[args.overrides] if args.overrides else {}
        display_config = (
            self.shell.user_ns[args.display] if args.display not in [True, None] else {}
        )

        # Decision: write to file before trying to build and execute Driver
        # See argument `help` for behavior details
        if args.write_to_file:
            if isinstance(args.write_to_file, str):
                file_path = Path(args.write_to_file)
            else:
                file_path = Path(f"{module_name}.py")
            file_path.write_text(cell)

        # create_module() is preferred over module_from_source() to simplify
        # the integration with the Hamilton UI which assumes physical Python modules
        cell_module = ad_hoc_utils.create_module(cell, module_name)
        self.shell.push({module_name: cell_module})

        # determine the Driver config
        if config and base_builder.config:
            return "AssertionError: Received a config -c/--config and a Builder -b/--builder with an existing config. Pass either one."

        # build the Driver. the Builder is copied to avoid conflict with the user namespace
        builder = base_builder.copy()
        dr = builder.with_config(config).with_modules(cell_module).build()

        # determine final vars
        if args.execute not in [True, None]:
            final_vars = self.shell.user_ns[args.execute]
        else:
            nodes = [n for n in dr.list_available_variables() if not n.is_external_input]
            final_vars = topological_sort(nodes)

        # visualize
        if args.display:
            # try/except `display_config` or inputs/overrides may be invalid
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
                print(f"Failed to display {e}.\n\nThe display config was: {display_config}")
                dot = dr.display_all_functions()

            # handle output environment
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
            results = {_normalize_result_names(name): value for name, value in results.items()}
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
    def insert_module(self, line):
        """Alias for `%module_to_cell`."""
        self.module_to_cell(line)

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

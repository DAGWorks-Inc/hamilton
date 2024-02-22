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

import json
import os
import sys
from pathlib import Path
from types import ModuleType

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.display import HTML, display

from hamilton import ad_hoc_utils, driver, lifecycle


def create_module(source: str, name: str = None) -> ModuleType:
    """Create a temporary module from source code"""
    module_name = ad_hoc_utils._generate_unique_temp_module_name() if name is None else name
    module_object = ModuleType(module_name, "")
    sys.modules[module_name] = module_object
    exec(source, module_object.__dict__)
    return module_object


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


def find_all_hamilton_drivers_using_this_module(shell, module_name: str) -> list:
    """Find all Hamilton drivers in the notebook that use the module `module_name`.

    :param shell: the ipython shell object
    :param module_name: the module name to search for
    :return: the list of (driver variable name, drivers) that use the module
    """
    driver_instances = {
        var_name: shell.user_ns[var_name]
        for var_name in shell.user_ns
        if isinstance(shell.user_ns[var_name], driver.Driver) and var_name != f"{module_name}_dr"
    }
    impacted_drivers = []
    for var_name, dr in driver_instances.items():
        for driver_module in dr.graph_modules:
            if driver_module.__name__ == module_name:
                impacted_drivers.append((var_name, dr))
                break
    return impacted_drivers


def rebuild_drivers(shell, module_name: str, module_object: ModuleType, verbosity: int = 1) -> dict:
    """Function to rebuild drivers that use the module `module_name` with the new module `module_object`.

    This finds the drivers and rebuilds them if it knows how. It will skip rebuilding if the driver has an adapter.

    :param shell:
    :param module_name:
    :param module_object:
    :param verbosity:
    :return:
    """
    impacted_drivers = find_all_hamilton_drivers_using_this_module(shell, module_name)
    drivers_rebuilt = {}
    for var_name, dr in impacted_drivers:
        modules_to_use = [mod for mod in dr.graph_modules if mod.__name__ != module_name]
        modules_to_use.append(module_object)
        # TODO: make this more robust by providing some better APIs.
        if (
            dr.adapter
            and hasattr(dr.adapter, "_adapters")
            and (
                not dr.adapter._adapters
                or isinstance(dr.adapter._adapters[0], lifecycle.base.LifecycleAdapterSet)
            )
        ):
            # TODO: make this more robust with a better API
            _config = dr.graph._config
            dr = (
                driver.Builder()
                .with_modules(*modules_to_use)
                .with_config(_config)
                .with_adapter(dr.adapter)
                .build()
            )
            drivers_rebuilt[var_name] = dr

            if verbosity > 0:
                print(
                    f"Rebuilt {var_name} with module {module_name}, using it's config of {_config}"
                )
        else:
            if verbosity > 0:
                print(f"Driver {var_name} has an adapter passed, skipping rebuild.")

    return drivers_rebuilt


def determine_notebook_type() -> str:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return "databricks"
    return "default"


@magics_class
class HamiltonMagics(Magics):
    """Magics to facilitate Hamilton development in Jupyter notebooks"""

    @magic_arguments()  # needed on top to enable parsing
    @argument(
        "-m", "--module_name", help="Module name to provide. Default is jupyter_module."
    )  # keyword / optional arg
    @argument(
        "-c", "--config", help="JSON config, or variable name containing config to use."
    )  # keyword / optional arg
    @argument(
        "-r", "--rebuild-drivers", action="store_true", help="Flag to rebuild drivers"
    )  # Flag / optional arg
    @argument(
        "-d", "--display", action="store_true", help="Flag to visualize dataflow."
    )  # Flag / optional arg
    @argument(
        "-v", "--verbosity", type=int, default=1, help="0 to hide. 1 is normal, default"
    )  # keyword / optional arg
    @cell_magic
    def cell_to_module(self, line, cell):
        """Execute the cell and dynamically create a Python module from its content.
        A Hamilton Driver is automatically instantiated with that module for variable `{MODULE_NAME}_dr`.

        > %%cell_to_module -m MODULE_NAME --display --rebuild-drivers
        """
        if "--help" in line.split():
            print("Help for %%cell_to_module magic:")
            print("  -m, --module_name: Module name to provide. Default is jupyter_module.")
            print("  -c, --config: JSON config string, or variable name containing config to use.")
            print("  -r, --rebuild-drivers: Flag to rebuild drivers.")
            print("  -d, --display: Flag to visualize dataflow.")
            print("  -v, --verbosity: of standard output. 0 to hide. 1 is normal, default.")
            return  # Exit early
        if not hasattr(self, "notebook_env"):
            # doing this so I don't have to deal with the constructor
            self.notebook_env = determine_notebook_type()
        # shell.ex() is equivalent to exec(), but in the user namespace (i.e. notebook context).
        # This allows imports and functions defined in the magic cell %%cell_to_module to be
        # directly accessed from the notebook
        self.shell.ex(cell)

        args = parse_argstring(self.cell_to_module, line)  # specify how to parse by passing method
        module_name = "jupyter_module" if args.module_name is None else args.module_name

        display_config = {}
        if args.config:
            if args.config in self.shell.user_ns:
                display_config = self.shell.user_ns[args.config]
            else:
                try:
                    if args.config.startswith("'") or args.config.startswith('"'):
                        # strip quotes if present
                        args.config = args.config[1:-1]
                    display_config = json.loads(args.config)
                except json.JSONDecodeError:
                    print("Failed to parse config as JSON. Please ensure it's a valid JSON string:")
                    print(args.config)

        module_object = create_module(cell, module_name)

        # shell.push() assign a variable in the notebook. The dictionary keys are variable name
        self.shell.push({module_name: module_object})

        # shell.user_ns is a dictionary of all variables in the notebook
        # rebuild drivers that use this module
        if args.rebuild_drivers:
            rebuilt_drivers = rebuild_drivers(
                self.shell, module_name, module_object, verbosity=args.verbosity
            )
            self.shell.user_ns.update(rebuilt_drivers)

        # create a driver to display things for every cell with %%with_functions
        dr = driver.Builder().with_modules(module_object).with_config(display_config).build()
        self.shell.push({f"{module_name}_dr": dr})
        if args.display:
            graphviz_obj = dr.display_all_functions()
            if self.notebook_env == "databricks" and graphviz_obj:
                try:
                    display(HTML(graphviz_obj.pipe(format="svg").decode("utf-8")))
                except Exception as e:
                    print(f"Failed to display graph: {e}")
                    print("Please ensure graphviz is installed via `%sh apt install -y graphviz`")
                return
            # return will go to the output cell. To display multiple elements, use
            # IPython.display.display(print("hello"), dr.display_all_functions(), ...)
            return graphviz_obj

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


def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    ipython.register_magics(HamiltonMagics)

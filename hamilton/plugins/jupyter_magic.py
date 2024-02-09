"""
Module for use with jupyter notebooks to facilitate Hamilton development.

Usage:
   > # To load it
   > %load_ext hamilton.plugins.jupyter_magic

   > %%with_functions -m MODULE_NAME
   > def my_hamilton_funcs(): ...

"""

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
    """Find all Hamilton drivers in the notebook that use the module `module_name`"""
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


def rebuild_drivers(shell, module_name: str, module_object: ModuleType) -> dict:
    impacted_drivers = find_all_hamilton_drivers_using_this_module(shell, module_name)
    drivers_rebuilt = {}
    for var_name, dr in impacted_drivers:
        modules_to_use = [mod for mod in dr.graph_modules if mod.__name__ != module_name]
        modules_to_use.append(module_object)
        if (
            dr.adapter
            and hasattr(dr.adapter, "_adapters")
            and (
                not dr.adapter._adapters
                or isinstance(dr.adapter._adapters[0], lifecycle.base.LifecycleAdapterSet)
            )
        ):
            _config = dr.graph._config
            dr = (
                driver.Builder()
                .with_modules(*modules_to_use)
                .with_config(_config)
                .with_adapter(dr.adapter)
                .build()
            )
            drivers_rebuilt[var_name] = dr

            print(f"Rebuilt {var_name} with module {module_name}, using config {_config}")
        else:
            print(f"Driver {var_name} has an adapter passed, skipping rebuild.")

    return drivers_rebuilt


@magics_class
class HamiltonMagics(Magics):
    """Magics to facilitate Hamilton development in Jupyter notebooks"""

    @magic_arguments()  # needed on top to enable parsing
    @argument("-m", "--module_name", help="Optional module name.")  # keyword / optional arg
    @cell_magic
    def with_functions(self, line, cell):
        """Execute the cell and dynamically create a Python module from its content.
        A Hamilton Driver is automatically instantiated with that module for variable `dr`.
        """
        # shell.ex() is equivalent to exec(), but in the user namespace (i.e. notbook context).
        # This allows imports and functions defined in the magic cell %%with_functions to be
        # directly accessed from the notebook
        self.shell.ex(cell)

        args = parse_argstring(self.with_functions, line)  # specify how to parse by passing method
        module_name = "jupyter_module" if args.module_name is None else args.module_name

        module_object = create_module(cell, module_name)

        # shell.push() assign a variable in the notebook. The dictionary keys are variable name
        self.shell.push({module_name: module_object})

        # shell.user_ns is a dictionary of all variables in the notebook
        # rebuild drivers that use this module
        rebuilt_drivers = rebuild_drivers(self.shell, module_name, module_object)
        self.shell.user_ns.update(rebuilt_drivers)

        # create a driver to display things for every cell with %%with_functions
        dr = driver.Builder().with_modules(module_object).build()
        self.shell.push({f"{module_name}_dr": dr})
        # return will go to the output cell. To display multiple elements, use
        # IPython.display.display(print("hello"), dr.display_all_functions(), ...)
        return dr.display_all_functions()

    @line_magic
    def insert_module(self, line):
        """Insert in the next cell the source code from the module (.py)
        at the path specified by `line`
        """

        # JavaScript magic to generate a new cell
        insert_cell_with_content()

        module_path = Path(line)
        # insert our custom %%with_functions magic at the top of the cell
        header = f"%%with_functions -m {module_path.stem}\n\n"
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

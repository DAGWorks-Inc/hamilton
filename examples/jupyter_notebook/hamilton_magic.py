import sys
from pathlib import Path
from types import ModuleType

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class, output_can_be_silenced
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.display import HTML, display

from hamilton import ad_hoc_utils, driver


def create_module(source: str) -> ModuleType:
    """Create a temporary module from source code"""
    module_name = ad_hoc_utils._generate_unique_temp_module_name()
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

        module_object = create_module(cell)

        # shell.push() assign a variable in the notebook. The dictionary keys are variable name
        self.shell.push({module_name: module_object})

        # shell.user_ns is a dictionary of all variables in the notebook
        if self.shell.user_ns.get("dr"):
            if not isinstance(self.shell.user_ns.get("dr"), driver.Driver):
                raise KeyError("The variable `dr` is already defined and is not a `driver.Driver`")

        # this design limits to "one driver / one `%%with_functions`" cell per notebook
        dr = driver.Builder().with_modules(module_object).build()
        self.shell.push({"dr": dr})
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

    @magic_arguments()
    @argument("-u", "--user", help="Optional user")  # keyword / optional arg
    @argument("dataflow", help="Dataflow name.")  # positional / required arg
    @line_magic
    @output_can_be_silenced
    def insert_dataflow(self, line):
        """Insert in the next cell the source code from a dataflow found on
        the dataflow hub. Specify the dataflow and the user (default is DAGWorks).
        Dataflow Hub: https://hub.dagworks.io/docs/

        same logic as `insert_module()` except it receives arguments to specify
        a dataflow
        """
        insert_cell_with_content()
        args = parse_argstring(self.insert_dataflow, line)

        # custom implementation of dataflows.import_dataflow (see below)
        module_path = import_dataflow(dataflow=args.dataflow, user=args.user)
        header = f"%%with_functions -m {args.dataflow}\n\n"
        module_source = module_path.read_text()
        # didn't include "__main__" because it could have other quotes  '__main__'
        module_source, _, _ = module_source.partition("if __name__ ==")
        module_source = module_source.strip()

        # TODO need to parse out the following section from __init__ files:
        # - logger
        # - contrib.catch_import_errors
        # - unindent under contrib.catch_import_errors
        # this should be done with ast parsing using shell.ast_transformers()
        # ref: https://ipython.readthedocs.io/en/stable/api/generated/IPython.core.magics.ast_mod.html
        # ref: https://ipython.readthedocs.io/en/stable/api/generated/IPython.core.inputtransformer2.html

        # insert source code as text in the next cell
        self.shell.set_next_input(header + module_source, replace=False)


def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    ipython.register_magics(HamiltonMagics)


# dataflows related
import os

from hamilton.dataflows import OFFICIAL_PATH, USER_PATH, latest_commit, pull_module


# this is a trimmed version of `dataflows.import_dataflow` that returns a local path
# since nested functions are import from dataflows, this should do proper telemetry
def import_dataflow(
    dataflow: str, user: str = None, version: str = "latest", overwrite: bool = False
) -> Path:
    """Pulls & imports dataflow code from github and returns a module."""
    if version == "latest":
        version = latest_commit(dataflow, user)
    if user:
        local_file_path = (
            USER_PATH.format(commit_ish=version, user=user, dataflow=dataflow) + "/__init__.py"
        )
    else:
        local_file_path = (
            OFFICIAL_PATH.format(commit_ish=version, dataflow=dataflow) + "/__init__.py"
        )
    if not os.path.exists(local_file_path) or overwrite:
        pull_module(dataflow, user, version=version, overwrite=overwrite)

    return Path(local_file_path)

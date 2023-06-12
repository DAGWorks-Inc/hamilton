import ast
import sys
from pathlib import Path
from types import ModuleType
from typing import Optional, Union

from hamilton import driver
from hamilton.ad_hoc_utils import _generate_unique_temp_module_name


class HamiltonRemover(ast.NodeTransformer):
    """Remove the decorators from functions nodes and hamilton imports

    visit() is a parent method that recursively visits nodes of the tree and returns
    an edited ast. Each visit_* methods apply to specific node types and should
    return a valid node. Returning None is equivalent to removing the node.
    """

    def visit_Import(self, node):
        """remove `import hamilton*`"""
        for alias in node.names:
            if alias.name.startswith("hamilton"):
                return None

        return node

    def visit_ImportFrom(self, node):
        """remove `from hamilton* import ...`"""
        if node.module.startswith("hamilton"):
            return None

        return node

    def visit_FunctionDef(self, node):
        """copies a node but empties decorator_list"""
        new_node = ast.copy_location(
            ast.FunctionDef(
                name=node.name,
                args=node.args,
                body=node.body,
                decorator_list=[],
                returns=node.returns,
            ),
            node,
        )
        return new_node


class HamiltonDecoratorReader(ast.NodeVisitor):
    """Read and store the decorators of each function

    visit() is a parent method that recursively visits nodes of the tree and returns
    a view of the ast. Each visit_* methods apply to specific node types and should
    return None. Subclasses of ast.NodeVisitor are readonly
    """

    def __init__(self):
        self.module_docstring: Optional[str] = None
        self.imports: list[ast.Import] = []
        self.imports_from: list[ast.ImportFrom] = []
        self.decorators: dict[str, list[ast.expr]] = {}

    def read_decorators(self, module_ast: ast.Module) -> ast.Module:
        """Main entrypoint; read the decorators and import of a file without editing it"""
        self.module_docstring = ast.get_docstring(module_ast, clean=True)
        self.visit(module_ast)
        return module_ast

    def visit_Import(self, node):
        """store all `import ...`
        The decorators may contain datatypes or module other than hamilton (e.g., pandas, numpy)
        """
        self.imports.append(node)

    def visit_ImportFrom(self, node):
        """store all `from ... import ...`
        The decorators may contain datatypes or module other than hamilton (e.g., pandas, numpy)
        """
        self.imports_from.append(node)

    def visit_FunctionDef(self, node):
        """Store the list of decorator objects (ast.Call) such that {function name: decorators}"""
        self.decorators[node.name] = node.decorator_list


class HamiltonDecoratorWriter(ast.NodeTransformer):
    """Add decorators to functions based on passed decorator mapping

    visit() is a parent method that recursively visits nodes of the tree and returns
    an edited ast. Each visit_* methods apply to specific node types and should
    return a valid node. Returning None is equivalent to removing the node.
    """

    def __init__(self):
        self.module_docstring: Optional[str] = None
        self.imports: list[ast.Import] = []
        self.imports_from: list[ast.ImportFrom] = []
        self.decorators: dict[str, list[ast.expr]] = {}

    def write_decorators(
        self,
        module_ast: ast.Module,
        hamilton_decorator_reader: HamiltonDecoratorReader,
    ) -> ast.Module:
        """Decorate an ast using a decorator mapping {function name: decorators}"""
        self.module_docstring = getattr(hamilton_decorator_reader, "module_docstring", None)
        self.imports = getattr(hamilton_decorator_reader, "imports", [])
        self.imports_from = getattr(hamilton_decorator_reader, "imports_from", [])
        self.decorators = getattr(hamilton_decorator_reader, "decorators", {})

        edited_module_ast = self.visit(module_ast)

        # the following code adds Module level code (docstring, import, import from)
        # the preserve the order, elements are inserted at index 0, in reserve order
        for import_from_ in self.imports_from[::-1]:
            edited_module_ast.body.insert(0, import_from_)

        for import_ in self.imports[::-1]:
            edited_module_ast.body.insert(0, import_)

        if self.module_docstring:
            edited_module_ast.body.insert(0, ast.Expr(ast.Constant(self.module_docstring)))

        return edited_module_ast

    def visit_FunctionDef(self, node):
        """copies a function node and decorates it using `decorators` passed to .decorate()
        Note. The added decorators will be "on top" of existing decorators, in the order passed.
        """
        new_decorator_list = self.decorators.get(node.name, []) + node.decorator_list

        new_node = ast.copy_location(
            ast.FunctionDef(
                name=node.name,
                args=node.args,
                body=node.body,
                decorator_list=new_decorator_list,
                returns=node.returns,
            ),
            node,
        )
        return new_node


def _get_module_path(module: Union[str, Path, ModuleType]) -> Path:
    if isinstance(module, str):
        module_path = Path(module)
    elif isinstance(module, Path):
        module_path = module
    elif isinstance(module, ModuleType):
        if not module.__spec__.has_location:
            raise FileNotFoundError(
                "Received a ModuleType object with .__spec__.has_location == False"
            )

        module_path = Path(module.__spec__.origin)
    else:
        raise TypeError("module needs to be `str`, `Path`, or `ModuleType`")

    return module_path


def _read_module_source(module_path: Union[str, Path]) -> str:
    with open(module_path, "r") as f:
        return f.read()


def _import_temporary_module(source: str, docstring: Optional[str] = None) -> ModuleType:
    module_name = _generate_unique_temp_module_name()
    module_object = ModuleType(module_name, docstring)
    sys.modules[module_name] = module_object
    exec(source, module_object.__dict__)

    return module_object


def remove_hamilton(module: Union[str, Path, ModuleType], save_module=False) -> ModuleType:
    """Remove the decorators from functions nodes and hamilton imports"""
    module_path = _get_module_path(module)
    source = _read_module_source(module_path)
    module_ast = ast.parse(source)
    edited_module_ast = HamiltonRemover().visit(module_ast)
    imported_module = _import_temporary_module(ast.unparse(edited_module_ast))

    if save_module:
        output_file_name = str(module_path.stem) + "__undecorated" + str(module_path.suffix)
        with open(output_file_name, "w") as f:
            f.write(ast.unparse(edited_module_ast))

    return imported_module


def use_external_decorators(
    functions_module: Union[str, ModuleType],
    decorators_module: Union[str, ModuleType],
    save_module: bool = False,
) -> ModuleType:
    """Add Hamilton decorators to a Python module based on another module

    functions_module: the module receiving Hamilton decorators
    decorators_module: the module providing Hamilton decorators
    save_module: will save the decorated module in the same directory as `functions_module`
    """

    functions_module_path = _get_module_path(functions_module)
    functions_source = _read_module_source(functions_module_path)
    functions_ast = ast.parse(functions_source)

    decorators_module_path = _get_module_path(decorators_module)
    decorators_source = _read_module_source(decorators_module_path)
    decorators_ast = ast.parse(decorators_source)

    reader = HamiltonDecoratorReader()
    reader.visit(decorators_ast)

    writer = HamiltonDecoratorWriter()
    decorated_functions_ast = writer.write_decorators(functions_ast, reader)

    decorated_functions_module = _import_temporary_module(ast.unparse(decorated_functions_ast))

    if save_module:
        output_file_name = (
            str(functions_module_path.stem) + "__decorated" + str(functions_module_path.suffix)
        )
        with open(output_file_name, "w") as f:
            f.write(ast.unparse(decorated_functions_ast))

    return decorated_functions_module


if __name__ == "__main__":
    # 1. remove hamilton dependencies from a file and save it (import and decorators)

    # read `my_functions.py` and parse the text file into an ast
    with open("my_functions.py", "r") as f:
        source_ast = ast.parse(f.read())

    undecorated_ast = HamiltonRemover().visit(source_ast)

    # convert the ast back to text and save it to `my_functions__undecorated.py`
    with open("my_functions__undecorated.py", "w") as f:
        f.write(ast.unparse(undecorated_ast))

    # 2. add Hamilton decorators to functions from another module

    # read `my_functions.py` and parse the text file into an ast
    with open("external_decorators.py", "r") as f:
        external_decorators_ast = ast.parse(f.read())

    # read `my_functions__undecorated.py` and parse the text file into an ast
    # with open("my_functions__undecorated.py", "r") as f:
    #     undecorated_ast = ast.parse(f.read())

    # collect the decorators from the external_decorators_ast
    reader = HamiltonDecoratorReader()
    reader.read_decorators(external_decorators_ast)

    # add the decorators to the undecorated_ast
    writer = HamiltonDecoratorWriter()
    decorated_ast = writer.write_decorators(undecorated_ast, reader)

    # 3. Save text / ast or load it into a Python module dynamically
    # convert the ast back to text and save it to `newly_decorated.py`
    with open("newly_decorated.py", "w") as f:
        f.write(ast.unparse(decorated_ast))

    # create empty ModuleType object; the ModuleType.name is registered in sys.modules
    module_object = ModuleType(
        "module_name", "module docstring"
    )  # you can use reader.module_docstring
    sys.modules["module_name"] = module_object
    # exec() binds the code text to the module_object
    exec(ast.unparse(decorated_ast), module_object.__dict__)

    dr = driver.Driver({}, module_object)
    dr.display_all_functions("hamilton.dot")

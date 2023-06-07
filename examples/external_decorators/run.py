import ast
import sys
import uuid
from types import ModuleType
from typing import Any, Union

from hamilton import driver


class HamiltonRemover(ast.NodeTransformer):
    """Remove the decorators from functions nodes

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
        self.module_docstring = None
        self.imports = []
        self.imports_from = []
        self.decorators = {}

    def visit_Module(self, node):
        self.module_docstring = ast.get_docstring(node, clean=True)

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
        self.module_docstring = None
        self.imports = []
        self.imports_from = []
        self.decorators = {}

    def decorate(
        self,
        tree,
        module_docstring,
        decorators: dict[str, list[ast.Call]],
        imports,
        imports_from,
    ) -> Any:
        """Decorate an ast using a decorator mapping {function name: decorators}"""
        self.module_docstring = module_docstring
        self.imports = imports
        self.imports_from = imports_from
        self.decorators = decorators
        return self.visit(tree)

    def visit_Module(self, node):
        for import_from_ in self.imports_from[::-1]:
            node.body.insert(0, import_from_)

        for import_ in self.imports[::-1]:
            node.body.insert(0, import_)

        if self.module_docstring:
            node.body.insert(0, ast.Expr(ast.Constant(self.module_docstring)))

        return node

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


# Read the script main to understand this function step by step
def use_external_decorators(
    undecorated_module: Union[str, ModuleType], external_decorators_module: Union[str, ModuleType]
) -> ModuleType:
    def _get_module_path(module):
        if isinstance(module, ModuleType):
            if not module.__spec__.has_location:
                raise FileNotFoundError

            module_path = module.__spec__.origin

        elif isinstance(module, str):
            module_path = module

        else:
            raise FileNotFoundError

        return module_path

    undecorated_module_path = _get_module_path(undecorated_module)
    external_decorators_module_path = _get_module_path(external_decorators_module)

    with open(undecorated_module_path, "r") as f:
        undecorated_ast = ast.parse(f.read())

    with open(external_decorators_module_path, "r") as f:
        external_decorators_ast = ast.parse(f.read())

    reader = HamiltonDecoratorReader()
    reader.visit(external_decorators_ast)

    writer = HamiltonDecoratorWriter()
    decorated_ast = writer.decorate(
        undecorated_ast,
        reader.module_docstring,
        reader.decorators,
        reader.imports,
        reader.imports_from,
    )

    module_uuid = str(uuid.uuid4())
    module_object = ModuleType(module_uuid, reader.module_docstring)
    sys.modules[module_uuid] = module_object
    exec(ast.unparse(decorated_ast), module_object.__dict__)

    return module_object


if __name__ == "__main__":
    # 1. remove hamilton dependencies from a file and save it (import and decorators)

    # read `my_functions.py` and parse the text file into an ast
    with open("my_functions.py", "r") as f:
        source_ast = ast.parse(f.read())

    undecorated_ast = HamiltonRemover().visit(source_ast)

    # convert the ast back to text and save it to `my_functions_no_decorators.py`
    with open("my_functions_no_hamilton.py", "w") as f:
        f.write(ast.unparse(undecorated_ast))

    # 2. add Hamilton decorators to functions from another module

    # read `my_functions.py` and parse the text file into an ast
    with open("external_decorators.py", "r") as f:
        external_decorators_ast = ast.parse(f.read())

    # read `my_functions_no_decorators.py` and parse the text file into an ast
    # with open("my_functions_.py", "r") as f:
    #     undecorated_ast = ast.parse(f.read())

    # collect the decorators from the external_decorators_ast
    reader = HamiltonDecoratorReader()
    reader.visit(external_decorators_ast)

    # add the decorators to the undecorated_ast
    writer = HamiltonDecoratorWriter()
    decorated_ast = writer.decorate(
        undecorated_ast,
        reader.module_docstring,
        reader.decorators,
        reader.imports,
        reader.imports_from,
    )

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

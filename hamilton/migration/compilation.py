import dataclasses
from typing import List, Optional, Dict, Union, Sequence, Tuple

import libcst as cst

from hamilton.migration import utils
from hamilton.migration.utils import get_subscript_name, get_name_name, raise_unsupported, SupportedType


@dataclasses.dataclass
class Transform:
    assigned_variable_name: str
    assigned_variable_type: SupportedType
    dependent_variables: List[str]
    # dependent_variables: Dict[str, Type[Type]]
    assign: cst.Assign = dataclasses.field(repr=False)  # The original assign expression -- will likely want to replace as we figure it out


@dataclasses.dataclass
class HamiltonFunction:
    input_types: Dict[str, SupportedType]
    output_type: SupportedType
    function_name: str
    function_comment: str
    function_body: Sequence[Union[cst.SimpleStatementLine, cst.BaseCompoundStatement]]

    # todo -- add decorators

    def to_function_def(self) -> cst.FunctionDef:
        out = cst.FunctionDef(
            name=cst.Name(value=self.function_name),
            params=cst.Parameters(
                params=[cst.Param(
                    name=cst.Name(value=param),
                    annotation=cst.Annotation(
                        annotation=type_.to_annotation()
                    )
                ) for param, type_ in self.input_types.items()]
            ),
            body=cst.IndentedBlock(
                body=self.function_body
            ),  # TODO -- ensure that this is the right type...
            returns=cst.Annotation(
                annotation=self.output_type.to_annotation()),
        )
        return out


# Commenting out as this is not necessary yet
# class TransformListOptimizer(abc.ABC):
#     @abc.abstractmethod
#     def optimize(self, transforms: List[Transform]) -> List[Transform]:
#         """Optimizes a set of transforms by running equivalence-transformations on it
#
#         @param transforms:
#         @return:
#         """


def get_name(assigned: Union[cst.Name, cst.Subscript]) -> str:
    if isinstance(assigned, cst.Name):
        # df = load_dataframe(...)
        # series = a + b
        # TODO -- respect type annotations
        return get_name_name(assigned)
    elif isinstance(assigned, cst.Subscript):
        return get_subscript_name(assigned)
    raise_unsupported("Invalid target for assignment. Only single assignments are allowed, E.G. df['a'] = ...", assigned)


class GatherTransforms(cst.CSTVisitor):
    def __init__(self):
        self._transforms = []
        self._variable_type_map = {}

    # def _derive_target_type_from_function_call(self, call: cst.Call):
    #     """Derives the target type from the function call. Best guess."""
    #     pass
    #
    # def _derive_target_name_from_assign_target(self, assign_target: cst.AssignTarget):
    #     pass

    @property
    def transforms(self) -> List[Transform]:
        return self._transforms

    @property
    def type_map(self) -> Dict[str, SupportedType]:
        return self._variable_type_map

    def visit_FunctionDef(self, node: "FunctionDef") -> Optional[bool]:
        return False # No need to dig into these

    def visit_AnnAssign(self, node: cst.AnnAssign) -> Optional[bool]:
        transform = self.convert_to_transform(node)
        self.remember_transform(transform)
        return False
        # return self.visit_Assign(node, type=SupportedType.from_annotation(node.annotation))

    def visit_Assign(self, node: cst.Assign) -> Optional[bool]:
        """Visits an assignment node. NOdes can come from lines of the form:
        1. df = load_df(...) # some function to load a dataframe or some piece of data
        2. df = ... # Some constant for the dataframe
        3. df['a'] = df['b'] + df['c'] # Some in-place modification of dataframes
        4. df.a = df.b + df['c'] # using a different shape to modify
        5. series = df.a + df.b # Not assigning, just creating series

        For now, we assume the following:
        1. That all "raw" variable assignemnts (case (1) and (2)) are dataframes
            -> we can adjust this as needed
            -> we will want to adjust to use (2) when there are constants that we can derive types for
        2. That all operations on series are type-preserving
            -> E.G> (3), (4), and (5) all output series

        This is a WIP but this should solve the 80% -- the 20% is not going to be solved by this

        @param node:
        @return:
        """
        transform = self.convert_to_transform(node)
        self.remember_transform(transform)
        return False

    def remember_transform(self, transform):
        if transform.assigned_variable_name in self._variable_type_map:
            assert self._variable_type_map[transform.assigned_variable_name] == transform.assigned_variable_type
        else:
            self._variable_type_map[transform.assigned_variable_name] = transform.assigned_variable_type
        self.transforms.append(transform)

    def _derive_dependents(self, value: cst.BaseExpression) -> List[str]:
        """A trick to derive dependents. The right way to do this would be to use libcst's
        traversal methodology although its unclear if its sophisticated enough to pull this off...

        @param value:
        @return:
        """
        dependents = []
        if isinstance(value, cst.Call):
            # a = f(b,c)
            for arg in value.args:
                dependents.extend(self._derive_dependents(arg))
        elif isinstance(value, cst.BinaryOperation):
            # a = b + c
            for operand in [value.left, value.right]:
                dependents.extend(self._derive_dependents(operand))
        elif isinstance(value, cst.Name):
            # a = b
            dependents.append(get_name(value))
        elif isinstance(value, cst.Subscript):
            value_name = get_name(value)
            dependents.append(value_name)
            attribute_of = value.value.value # this is a little silly and what I get for calling vars "value"
            subscript_of_type = self.type_map[attribute_of]
            self.type_map[value_name] = SupportedType.subscript(subscript_of_type)
            # import pdb
            # pdb.set_trace()
        elif isinstance(value, cst.Name):
            dependents.append(get_name_name(value))
            # import pdb
            # pdb.set_trace()
        elif isinstance(value, (cst.SimpleString, cst.Integer, cst.Float)):
            pass  # TODO -- log
        else:
            pass
            # raise_unsupported("Invalid value for assignment. Only binary (or multiple) operators are allowed or function calls = ...", value)
        return dependents

    def convert_to_transform(self, node: Union[cst.Assign, cst.AnnAssign]) -> Transform:
        type_ = None
        if isinstance(node, cst.Assign):
            if len(node.targets) != 1:
                raise ValueError(f"Assignments can only have one target")
            target, = node.targets
            target = target.target
        elif isinstance(node, cst.AnnAssign):
            target = node.target
            type_= SupportedType.from_annotation(annotation=node.annotation)
        else:
            raise Exception(f"{node.__class__}")
        value = node.value
        var_name = get_name(target)
        dependents = self._derive_dependents(node.value)
        if type_ is None:
            type_ = utils.derive_types_from_dependent_types([self._variable_type_map[dependent] for dependent in dependents if dependent in self._variable_type_map])
        # TODO -- derive the type from the type of the dependents
        # All series -> series
        # Others to Any
        return Transform(
            var_name,
            assigned_variable_type=type_,
            dependent_variables=dependents,
            assign=node)  # TODO -- fix this


def gather_transforms(contents: cst.Module) -> Tuple[List[Transform], Dict[str, SupportedType]]:
    """Gathers all transformations from a list of lines

    @param contents:
    @return:
    """
    module = cst.ensure_type(contents, cst.Module)
    transformer = GatherTransforms()
    module.visit(transformer)
    return transformer.transforms, transformer.type_map


def _convert_to_hamilton_function_body(assign: cst.Assign, transforms: List[Transform]) -> Sequence[cst.SimpleStatementLine]:
    value = assign.value

    class TransformValueToInputVariable(cst.CSTTransformer):
        def __init__(self, all_transforms: List[Transform]):
            self.transforms = all_transforms

        def leave_Subscript(
                self, original_node: "Subscript", updated_node: "Subscript"
        ) -> "BaseExpression":
            return cst.Name(value=utils.get_subscript_name(original_node))

    # class TransformToInputVariables(cst.CSTTransformer):
    #     def leave_Assign(
    #             self, original_node: cst.Assign, updated_node: cst.Assign
    #     ) -> Any:
    #         import pdb
    #         pdb.set_trace()
    #         return TransformValueToInputVariable()
    #         # return cst.SimpleStatementLine(
    #         #     body=[
    #         #         cst.Return(
    #         #             value=cst.SimpleString("'TODO -- make me the actual code...'"))]
    #         # )
    # print(utils.to_code(value))
    transformed = value.visit(TransformValueToInputVariable(transforms))
    return [cst.SimpleStatementLine(body=[
        cst.Return(
            value=transformed
        )
    ])]


def convert_transforms(transforms: List[Transform], type_map: Dict[str, SupportedType]) -> List[HamiltonFunction]:
    """Compiles the transforms into a list of hamilton functions.
    Note that this already optimizes them. We will likely want another
    abstraction here to improve this.

    @param transforms: Transforms to compile to hamilton functions
    @return:
    """
    out = []
    for transform in transforms:
        out.append(HamiltonFunction(
            input_types={
                key: type_map.get(key, SupportedType.ANY) for key in transform.dependent_variables},  # Using `Any` is a hack here
            output_type=transform.assigned_variable_type,
            function_name=transform.assigned_variable_name,
            function_comment=f"Automatically derived function for {transform.assigned_variable_name}",
            function_body=_convert_to_hamilton_function_body(transform.assign, transforms)
        ))
    return out


def rerender(original_contents: cst.Module, hamilton_functions: List[HamiltonFunction], nodes_to_replace: List[cst.CSTNode]) -> str:
    """Rerenders the original contents into the file.

    Note that this is missing some metadata to say *where* they came from.
    We'll want to add this soon.

    Honestly, this should probably come from CST visitor/transformer pattern, but TBD.
    This is just to prove the concept for now.
    The nodes_to_replace arg should be replaced by using the actual transform

    @param original_contents:
    @param hamilton_functions:
    @return: The final contents of the file
    """
    # module = original_contents.deep_clone()  # Create a deep clone
    new_body = []
    for body_item in original_contents.body:
        for node_to_replace in nodes_to_replace:
            should_remove = False
            if isinstance(body_item, cst.SimpleStatementLine) and isinstance(body_item.body[0], cst.Assign):
                if body_item.body[0].deep_equals(node_to_replace):  # Meaning its fully equal
                    should_remove = True
                    break
        if not should_remove:
            new_body.append(body_item)
    new_body.extend([fn.to_function_def() for fn in hamilton_functions])  # this is sloppy but we're just adding them onto the end
    return cst.Module(
        header=original_contents.header,
        footer=original_contents.footer,
        body=new_body,
        encoding=original_contents.encoding,
        default_indent=original_contents.default_indent,
        default_newline=original_contents.default_newline,
        has_trailing_newline=original_contents.has_trailing_newline
    ).code


def transpile_contents(contents: str) -> str:
    """Transpiles contents from dataframe manipulations to a hamilton DAG.
    Heh this whole file could be written using hamilton...

    @param contents: Contents to convert.
    @return: A string representing a hamilton DAG. Should be valid python code.
    """
    module = cst.parse_module(contents)
    # import pdb
    # pdb.set_trace()
    transforms, type_map = gather_transforms(module)
    # TODO -- add optimizer call
    functions = convert_transforms(transforms, type_map)
    return rerender(module, functions, [transform.assign for transform in transforms])

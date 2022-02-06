import ast
from typing import Dict, Callable, Any, Collection, Type

from hamilton import node
from hamilton.function_modifiers_base import NodeTransformer, sanitize_function_name
from hamilton.node import DependencyType


class augment(NodeTransformer):
    def __init__(self, expr: str, output_type: Type[Type] = None):
        """Takes in an expression to transform a node. Expression must be of the form: foo=...
        Note that this is potentially unsafe!!! We use `eval` in this, so if you execute hamilton code with this
        it could easily carry out extra code. Keep this in mind when you use @augment -- utilize at your own peril.

        :param expr: expression to transform the node. Must have a variable in it with the function's name.
        :param output_type: Type of the output, if it differs from the node. Make sure to specify this
        if it differs or the types will not compile.
        """
        self.expr = expr
        self.expression = expr
        self.output_type = output_type

    def transform_node(self, node_: node.Node, config: Dict[str, Any], fn: Callable) -> Collection[node.Node]:
        """Transforms a node using the expression.

        :param node_: Node to transform
        :param output_type: The type of the node output. If it changes, then we specify it as Any
        :return: The collection of nodes that are outputted by this transform. Will be an extra node
        with the expression applied to the result of the original node.
        """

        expression_parsed = ast.parse(self.expression, mode='eval')
        dependent_variables = {item.id for item in ast.walk(expression_parsed) if isinstance(item, ast.Name)}

        var_name = sanitize_function_name(fn.__name__)

        # We should be passing the function through (or the name?)

        def new_function(**kwargs):
            kwargs_for_original_call = {key: value for key, value in kwargs.items() if key in node_.input_types}
            unmodified_result = node_.callable(**kwargs_for_original_call)
            kwargs_for_new_call = {key: value for key, value in kwargs.items() if key in dependent_variables}
            kwargs_for_new_call[var_name] = unmodified_result
            return eval(self.expression, kwargs_for_new_call)

        replacement_node = node.Node(
            name=node_.name,
            typ=node_.type if self.output_type is None else self.output_type,
            doc_string=node_.documentation,
            callabl=new_function,
            node_source=node_.node_source,
            input_types={
                **{dependent_var: (Any, DependencyType.REQUIRED) for dependent_var in dependent_variables if dependent_var != var_name},
                **{param: (input_type, dependency_type) for param, (input_type, dependency_type) in node_.input_types.items()}
            }
        )
        return [replacement_node]

        # TODO -- find a cleaner way to to copy a node

    def validate(self, fn: Callable):
        """Validates the expression is of the right form"""
        parsed_expression = ast.parse(self.expression, mode='eval')
        if not isinstance(parsed_expression, ast.Expression):
            raise ValueError(f'Expression {self.expr} must be an expression.')
        dependent_variables = {item.id for item in ast.walk(parsed_expression) if isinstance(item, ast.Name)}
        var_name = sanitize_function_name(fn.__name__)
        if var_name not in dependent_variables:
            raise ValueError(f'Expression must depend on the function its transforming. Did not find {var_name} in your expression\'s AST '
                             f'If you have a function called "foo", your expression must be a function of foo (as well as other variables). '
                             f'If you want to replace the value of this function, write your function as you normally would (E.G. as a function of its parameters).')

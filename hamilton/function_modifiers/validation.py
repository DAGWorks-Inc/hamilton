import abc
from typing import Any, Callable, Collection, Dict, List, Type

from hamilton import node
from hamilton.data_quality import base as dq_base
from hamilton.data_quality import default_validators
from hamilton.function_modifiers import base

"""Decorators that validate artifacts of a node"""

IS_DATA_VALIDATOR_TAG = "hamilton.data_quality.contains_dq_results"
DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG = "hamilton.data_quality.source_node"


class BaseDataValidationDecorator(base.NodeTransformer):
    @abc.abstractmethod
    def get_validators(self, node_to_validate: node.Node) -> List[dq_base.DataValidator]:
        """Returns a list of validators used to transform the nodes.

        :param node_to_validate: Nodes to which the output of the validator will apply
        :return: A list of validators to apply to the node.
        """
        pass

    def transform_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        raw_node = node.Node(
            name=node_.name
            + "_raw",  # TODO -- make this unique -- this will break with multiple validation decorators, which we *don't* want
            typ=node_.type,
            doc_string=node_.documentation,
            callabl=node_.callable,
            node_source=node_.node_source,
            input_types=node_.input_types,
            tags=node_.tags,
        )
        validators = self.get_validators(node_)
        validator_nodes = []
        validator_name_map = {}
        for validator in validators:

            def validation_function(validator_to_call: dq_base.DataValidator = validator, **kwargs):
                result = list(kwargs.values())[0]  # This should just have one kwarg
                return validator_to_call.validate(result)

            validator_node_name = node_.name + "_" + validator.name()
            validator_node = node.Node(
                name=validator_node_name,  # TODO -- determine a good approach towards naming this
                typ=dq_base.ValidationResult,
                doc_string=validator.description(),
                callabl=validation_function,
                node_source=node.NodeSource.STANDARD,
                input_types={raw_node.name: (node_.type, node.DependencyType.REQUIRED)},
                tags={
                    **node_.tags,
                    **{
                        base.NodeTransformer.NON_FINAL_TAG: True,  # This is not to be used as a subdag later on
                        IS_DATA_VALIDATOR_TAG: True,
                        DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG: node_.name,
                    },
                },
            )
            validator_name_map[validator_node_name] = validator
            validator_nodes.append(validator_node)

        def final_node_callable(
            validator_nodes=validator_nodes, validator_name_map=validator_name_map, **kwargs
        ):
            """Callable for the final node. First calls the action on every node, then

            :param validator_nodes:
            :param validator_name_map:
            :param kwargs:
            :return: returns the original node output
            """
            failures = []
            for validator_node in validator_nodes:
                validator: dq_base.DataValidator = validator_name_map[validator_node.name]
                validation_result: dq_base.ValidationResult = kwargs[validator_node.name]
                if validator.importance == dq_base.DataValidationLevel.WARN:
                    dq_base.act_warn(node_.name, validation_result, validator)
                else:
                    failures.append((validation_result, validator))
            dq_base.act_fail_bulk(node_.name, failures)
            return kwargs[raw_node.name]

        final_node = node.Node(
            name=node_.name,
            typ=node_.type,
            doc_string=node_.documentation,
            callabl=final_node_callable,
            node_source=node_.node_source,
            input_types={
                raw_node.name: (node_.type, node.DependencyType.REQUIRED),
                **{
                    validator_node.name: (validator_node.type, node.DependencyType.REQUIRED)
                    for validator_node in validator_nodes
                },
            },
            tags=node_.tags,
        )
        return [*validator_nodes, final_node, raw_node]

    def validate(self, fn: Callable):
        pass


class check_output_custom(BaseDataValidationDecorator):
    def __init__(self, *validators: dq_base.DataValidator):
        """Creates a check_output_custom decorator. This allows
        passing of custom validators that implement the DataValidator interface.

        :param validator: Validator to use.
        """
        self.validators = list(validators)

    def get_validators(self, node_to_validate: node.Node) -> List[dq_base.DataValidator]:
        return self.validators


class check_output(BaseDataValidationDecorator):
    def get_validators(self, node_to_validate: node.Node) -> List[dq_base.DataValidator]:
        return default_validators.resolve_default_validators(
            node_to_validate.type,
            importance=self.importance,
            available_validators=self.default_decorator_candidates,
            **self.default_validator_kwargs,
        )

    def __init__(
        self,
        importance: str = dq_base.DataValidationLevel.WARN.value,
        default_decorator_candidates: Type[dq_base.BaseDefaultValidator] = None,
        **default_validator_kwargs: Any,
    ):
        """Creates the check_output validator. This constructs the default validator class.
        Note that this creates a whole set of default validators
        TODO -- enable construction of custom validators using check_output.custom(*validators)

        :param importance: For the default validator, how important is it that this passes.
        :param validator_kwargs: keyword arguments to be passed to the validator
        """
        self.importance = importance
        self.default_validator_kwargs = default_validator_kwargs
        self.default_decorator_candidates = default_decorator_candidates
        # We need to wait until we actually have the function in order to construct the validators
        # So, we'll just store the constructor arguments for now and check it in validation

    @staticmethod
    def _validate_constructor_args(
        *validator: dq_base.DataValidator, importance: str = None, **default_validator_kwargs: Any
    ):
        if len(validator) != 0:
            if importance is not None or len(default_validator_kwargs) > 0:
                raise ValueError(
                    "Can provide *either* a list of custom validators or arguments for the default validator. "
                    "Instead received both."
                )
        else:
            if importance is None:
                raise ValueError("Must supply an importance level if using the default validator.")

    def validate(self, fn: Callable):
        """Validates that the check_output node works on the function on which it was called

        :param fn: Function to validate
        :raises: InvalidDecoratorException if the decorator is not valid for the function's return type
        """
        pass

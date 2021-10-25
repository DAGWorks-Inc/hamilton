import logging
from datetime import datetime
from typing import Dict, Collection, List, Any
from types import ModuleType

import pandas as pd

# required if we want to run this code stand alone.
import typing

from dataclasses import dataclass

if __name__ == '__main__':
    import graph
    import node
else:
    from . import graph
    from . import node

logger = logging.getLogger(__name__)


@dataclass
class Variable:
    """External facing API for hamilton. Having this as a dataclass allows us
    to hide the internals of the system but expose what the user might need.
    Furthermore, we can always add attributes and maintain backwards compatibility."""
    name: str
    type: typing.Type


class Driver(object):
    """This class orchestrates creating and executing the DAG to create a dataframe."""

    def __init__(self, config: Dict[str, Any], *modules: ModuleType, executor=None):
        """Constructor: creates a DAG given the configuration & modules to crawl.

        :param config: This is a dictionary of initial data & configuration.
                       The contents are used to help create the DAG.
        :param modules: Python module objects you want to inspect for Hamilton Functions.
        """
        if executor is None:
            executor = DirectExecutor()

        self.graph = graph.FunctionGraph(*modules, config=config, executor=executor)
        self.executor = executor

    def validate_inputs(self, user_nodes: Collection[node.Node], inputs: Dict[str, Any]):
        """Validates that inputs meet our expectations.

        :param user_nodes: The required nodes we need for computation.
        :param inputs: the user inputs provided.
        """
        # validate inputs
        errors = []
        for user_node in user_nodes:
            if user_node.name not in inputs:
                errors.append(f'Error: Required input {user_node.name} not provided for nodes: {[node.name for node in user_node.depended_on_by]}.')
            elif inputs[user_node.name] is not None and not self.executor.check_input_type(user_node, inputs[user_node.name]):
                errors.append(f'Error: Type requirement mismatch. Expected {user_node.name}:{user_node.type} '
                              f'got {inputs[user_node.name]} instead.')
        if errors:
            errors.sort()
            error_str = f'{len(errors)} errors encountered:\n  ' + '\n  '.join(errors)
            raise ValueError(error_str)

    def execute(self,
                final_vars: List[str],
                overrides: Dict[str, Any] = None,
                display_graph: bool = False) -> pd.DataFrame:
        """Executes computation.

        :param final_vars: the final list of variables we want in the data frame.
        :param overrides: the user defined input variables.
        :param display_graph: whether we want to display the graph being computed.
        :return: a data frame consisting of the variables requested.
        """
        columns = self.raw_execute(final_vars, overrides, display_graph)
        return pd.DataFrame(columns)

    def raw_execute(self,
                    final_vars: List[str],
                    overrides: Dict[str, Any] = None,
                    display_graph: bool = False) -> Dict[str, Any]:
        """Raw execute function that does the meat of execute.

        It does not try to stitch anything together. Thus allowing wrapper executes around this to shape the output
        of the data.

        :param final_vars:
        :param overrides:
        :param display_graph:
        :return:
        """
        nodes, user_nodes = self.graph.get_required_functions(final_vars)
        self.validate_inputs(user_nodes, self.graph.config)  # TODO -- validate within the function graph itself
        if display_graph:
            # TODO: fix hardcoded path.
            try:
                self.graph.display(nodes, user_nodes, output_file_path='test-output/execute.gv')
            except ImportError as e:
                logger.warning(f'Unable to import {e}', exc_info=True)
        memoized_computation = dict()  # memoized storage
        self.graph.execute(nodes, memoized_computation, overrides)
        columns = {c: memoized_computation[c] for c in final_vars}  # only want request variables in df.
        del memoized_computation  # trying to cleanup some memory
        return columns

    def list_available_variables(self) -> List[Variable]:
        """Returns available variables.

        :return: list of available variables.
        """
        return [Variable(node.name, node.type) for node in self.graph.get_nodes()]

    def display_all_functions(self):
        """Displays the graph."""
        self.graph.display_all()


class DirectExecutor:
    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        return node.type == typing.Any or isinstance(input, node.type)

    def execute(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return node.callable(**kwargs)


if __name__ == '__main__':
    """some example test code"""
    import sys
    import importlib

    formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(name)s(%(lineno)s): %(message)s')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

    if len(sys.argv) < 2:
        logger.error('No modules passed')
        sys.exit(1)
    logger.info(f'Importing {sys.argv[1]}')
    module = importlib.import_module(sys.argv[1])

    x = pd.date_range('2019-01-05', '2020-12-31', freq='7D')
    x.index = x

    dr = Driver({
        'VERSION': 'kids', 'as_of': datetime.strptime('2019-06-01', '%Y-%m-%d'),
        'end_date': '2020-12-31', 'start_date': '2019-01-05',
        'start_date_d': datetime.strptime('2019-01-05', '%Y-%m-%d'),
        'end_date_d': datetime.strptime('2020-12-31', '%Y-%m-%d'),
        'segment_filters': {'business_line': 'womens'}
    }, module, {})
    df = dr.execute(
        [
            'date_index',
            'some_column'
        ]
        # ,overrides={'DATE': pd.Series(0)}
        , display_graph=False)
    print(df)

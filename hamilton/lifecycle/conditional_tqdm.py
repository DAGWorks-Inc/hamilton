from typing import Any, Dict, List, Optional

import tqdm

from hamilton import graph, node
from hamilton.lifecycle import base


class TQDMHook(
    base.BasePreGraphExecute,
    base.BasePreNodeExecute,
    base.BasePostNodeExecute,
    base.BasePostGraphExecute,
):
    """A hook that uses tqdm to show progress bars for the graph execution.

    Note: you need to have tqdm installed for this to work.
    If you don't have it installed, you can install it with `pip install tqdm`.

    .. code-block:: python

        from hamilton import lifecycle

        dr = (
            driver.Builder()
            .with_config({})
            .with_modules(some_modules)
            .with_adapters(lifecycle.TQDMHook(desc="DAG-NAME"))
            .build()
        )
        # and then when you call .execute() or .materialize() you'll get a progress bar!


    """

    def __init__(self, desc: str = "Graph execution", node_name_target_width: int = 30, **kwargs):
        """Create a new TQDMHook.

        :param desc: The description to show in the progress bar. E.g. DAG Name is a good choice.
        :param kwargs: Additional kwargs to pass to TQDM. See TQDM docs for more info.
        :param node_name_target_width: the target width for the node name so that the progress bar is consistent.
        """
        self.desc = desc
        self.kwargs = kwargs
        self.node_name_target_width = node_name_target_width  # what we target padding for.

    def pre_graph_execute(
        self,
        *,
        run_id: str,
        graph: graph.FunctionGraph,
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
    ):
        all, human = graph.get_upstream_nodes(final_vars)
        total_node_to_execute = len(all) - len(human)
        self.progress_bar = tqdm.tqdm(
            desc=self.desc, unit="funcs", total=total_node_to_execute, **self.kwargs
        )

    def pre_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        task_id: Optional[str] = None,
    ):
        # pad the node name with spaces to make it consistent in length
        name_part = node_.name
        if len(name_part) > self.node_name_target_width:
            padding = ""
        else:
            padding = " " * (self.node_name_target_width - len(name_part))
        self.progress_bar.set_description_str(f"{self.desc} -> {name_part + padding}")
        self.progress_bar.set_postfix({"executing": name_part})

    def post_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        success: bool,
        error: Optional[Exception],
        result: Optional[Any],
        task_id: Optional[str] = None,
    ):
        self.progress_bar.update(1)

    def post_graph_execute(
        self,
        *,
        run_id: str,
        graph: graph.FunctionGraph,
        success: bool,
        error: Optional[Exception],
        results: Optional[Dict[str, Any]],
    ):
        name_part = "Execution Complete!"
        padding = " " * (self.node_name_target_width - len(name_part))
        self.progress_bar.set_description_str(f"{self.desc} -> {name_part + padding}")
        self.progress_bar.set_postfix({})
        self.progress_bar.close()

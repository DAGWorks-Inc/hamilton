from typing import Any, Dict, Optional

import narwhals as nw

from hamilton.lifecycle import api


class NarwhalsAdapter(api.NodeExecutionMethod):

    def run_to_execute_node(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_callable: Any,
        node_kwargs: Dict[str, Any],
        task_id: Optional[str],
        **future_kwargs: Any,
    ) -> Any:
        """This method is responsible for executing the node and returning the result.

        :param node_name: Name of the node.
        :param node_tags: Tags of the node.
        :param node_callable: Callable of the node.
        :param node_kwargs: Keyword arguments to pass to the node.
        :param task_id: The ID of the task, none if not in a task-based environment
        :param future_kwargs: Additional keyword arguments -- this is kept for backwards compatibility
        :return: The result of the node execution -- up to you to return this.
        """
        nw_kwargs = {}
        if "nw_kwargs" in node_tags:
            nw_kwargs = {k: True for k in node_tags["nw_kwargs"]}
        nw_func = nw.narwhalify(node_callable, **nw_kwargs)
        return nw_func(**node_kwargs)

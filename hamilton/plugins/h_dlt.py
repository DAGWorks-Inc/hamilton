import logging
from typing import Any, Dict, List, Literal, Optional, Union

import dlt

from hamilton import htypes
from hamilton.graph_types import HamiltonGraph
from hamilton.lifecycle import GraphExecutionHook
from hamilton.plugins.dlt_extensions import DltSaver

logger = logging.getLogger(__name__)


class DltAdapter(GraphExecutionHook):
    def __init__(
        self,
        pipeline: dlt.Pipeline,
        rename: Optional[dict] = None,
        write_disposition: Optional[Literal["skip", "append", "replace", "merge"]] = None,
        show_load_info: bool = False,
    ):
        """

        This is the preferred way to interface between dlt and Hamilton. All nodes requested for execution
        via `final_vars` will be materialized via a single pipeline.

        If you need more control, you can use the `DltSaver` from `hamilton.plugins.dlt_extensions` directly
        to dispatch materialize single or group of nodes to specific pipelines.
        """
        self.pipeline = pipeline
        self.rename = rename
        self.write_disposition = write_disposition
        self.show_load_info = show_load_info

    def run_before_graph_execution(self, *, graph: HamiltonGraph, final_vars: List[str], **kwargs):
        self.final_vars = final_vars

        valid_type_union = Union[tuple(DltSaver.applicable_types())]
        for node in graph.nodes:
            if node.name not in final_vars:
                continue

            # TODO this check by the lifecycle hook can't raise an error to prevent execution
            if htypes.custom_subclass_check(node.type, valid_type_union) is False:
                logger.warn(
                    f"Requested node `{node.name}` isn't a supported type by `DltAdapter`: {valid_type_union}"
                )

        # TODO add logic to handle sources

    # TODO why is `results` typed optional by default?
    def run_after_graph_execution(self, *, results: Dict[str, Any], **kwargs):
        data_saver = DltSaver(
            pipeline=self.pipeline,
            rename=self.rename,
            write_disposition=self.write_disposition,
        )
        load_info: dict = data_saver.save_data(results)

        if self.show_load_info:
            print(load_info)

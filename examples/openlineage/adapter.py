import json
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import attr
from openlineage.client import OpenLineageClient, event_v2, facet_v2

from hamilton import graph as h_graph
from hamilton import graph_types, node
from hamilton.lifecycle import base


@attr.s
class HamiltonFacet(facet_v2.RunFacet):
    hamilton_run_id: str = attr.ib()
    graph_version: str = attr.ib()
    final_vars: List[str] = attr.ib()
    inputs: List[str] = attr.ib()
    overrides: List[str] = attr.ib()


def get_stack_trace(exception):
    return "".join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


class OpenLineageAdapter(
    base.BasePreGraphExecute,
    base.BasePreNodeExecute,
    base.BasePostNodeExecute,
    base.BasePostGraphExecute,
):
    """
    This adapter emits OpenLineage events.

    1. We need to use materializer metadata to capture data inputs and outputs.
    2. The "job" emitted will be the graph.

    A Run will be emitted for each graph execution.
     - pre_graph_execute will emit a START event.
     - post_graph_execute will emit a COMPLETE event  (or others https://openlineage.io/docs/spec/run-cycle#run-states)
      - adding errorMessage facet if the graph execution failed.
     - post_node_execute will emit a RUNNING event with updates on input/outputs.

    A Job Event will be emitted for graph execution:
     - pre_graph_execute will:
      - emit the sourceCode Facet for the entire DAG as the job.
      - emit the sourceCodeLocation Facet for the entire DAG as the job.
     - post_node_execute will:
       - emit the SQLJob facet if data was loaded from a SQL source.
     it should emit a job type facet indicating Hamilton

    A Dataset Event will be emitted for:
     - post_node_execute will:
       - emit dataset event with schema facet, dataSource facet, lifecyclestate change facet,version facet,
       - input data sets when loading data - will have dataQualityMetrics facet, dataQualityAssertions facet (optional)
       - output data sets will have outputStatistics facet


    """

    def __init__(self, client: OpenLineageClient, namespace: str, job_name: str):
        """
        You pass in the OLClient.

        :param self:
        :param client:
        :param namespace:
        :param job_name:
        :return:
        """
        # self.transport = transport
        self.client = client
        self.namespace = namespace
        self.job_name = job_name

    def pre_graph_execute(
        self,
        run_id: str,
        graph: h_graph.FunctionGraph,
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
    ):
        """
        emits a Run START event.
        emits a Job Event with the sourceCode Facet for the entire DAG as the job.

        :param run_id:
        :param graph:
        :param final_vars:
        :param inputs:
        :param overrides:
        :return:
        """
        exportable_graph = graph_types.HamiltonGraph.from_graph(graph)
        graph_version = exportable_graph.version
        node_dict = [n.as_dict() for n in exportable_graph.nodes]
        job = event_v2.Job(
            namespace=self.namespace,
            name=self.job_name,
            facets={
                "sourceCode": facet_v2.source_code_job.SourceCodeJobFacet(
                    language="python",
                    sourceCode=json.dumps(node_dict),
                ),
                "jobType": facet_v2.job_type_job.JobTypeJobFacet(
                    processingType="BATCH",
                    integration="Hamilton",
                    jobType="DAG",
                ),
            },
        )
        run = event_v2.Run(
            runId=run_id,
            facets={
                "hamilton": HamiltonFacet(
                    hamilton_run_id=run_id,
                    graph_version=graph_version,
                    final_vars=final_vars,
                    inputs=list(inputs.keys()) if inputs else [],
                    overrides=list(overrides.keys()) if overrides else [],
                )
            },
        )
        run_event = event_v2.RunEvent(
            eventType=event_v2.RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=run,
            job=job,
        )
        self.client.emit(run_event)

    def pre_node_execute(
        self, run_id: str, node_: node.Node, kwargs: Dict[str, Any], task_id: Optional[str] = None
    ):
        """

        :param run_id:
        :param node_:
        :param kwargs:
        :param task_id:
        :return:
        """
        pass

    def post_node_execute(
        self,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        success: bool,
        error: Optional[Exception],
        result: Optional[Any],
        task_id: Optional[str] = None,
    ):
        """
        Run Event:
          - post_node_execute will emit a RUNNING event with updates on input/outputs.

        A Job Event will be emitted for graph execution:
         - post_node_execute will:
           - emit the SQLJob facet if data was loaded from a SQL source.
         it should emit a job type facet indicating Hamilton

        A Dataset Event will be emitted for:
         - post_node_execute will:
           - emit dataset event with schema facet, dataSource facet, lifecyclestate change facet,version facet,
           - input data sets when loading data - will have dataQualityMetrics facet, dataQualityAssertions facet (optional)
           - output data sets will have outputStatistics facet

        :param run_id:
        :param node_:
        :param kwargs:
        :param success:
        :param error:
        :param result:
        :param task_id:
        :return:
        """
        if not success:
            return
        metadata = {}
        saved_or_loaded = ""
        if node_.tags.get("hamilton.data_saver") is True and isinstance(result, dict):
            metadata = result
            saved_or_loaded = "saved"
        elif (
            node_.tags.get("hamilton.data_loader") is True
            and node_.tags.get("hamilton.data_loader.has_metadata") is True
            and isinstance(result, tuple)
            and len(result) == 2
            and isinstance(result[1], dict)
        ):
            metadata = result[1]
            saved_or_loaded = "loaded"
        if not metadata:
            return

        """
        TODO: create input dataset if appropriate
        create output dataset if appropriate
        Do the correct thing based on whether it was SQL or not...
        """
        print(metadata)
        inputs = []
        outputs = []
        if saved_or_loaded == "loaded":
            if "file_metadata" in metadata:
                name = node_.name
            elif "sql_metadata" in metadata:
                name = metadata["sql_metadata"]["table_name"]
            else:
                name = "--UNKNOWN--"
            inputs = [event_v2.InputDataset(self.namespace, name)]
        else:
            if "file_metadata" in metadata:
                name = metadata["file_metadata"]["path"]
            elif "sql_metadata" in metadata:
                name = metadata["sql_metadata"]["table_name"]
            else:
                name = "--UNKNOWN--"
            outputs = [event_v2.OutputDataset(self.namespace, name)]

        run = event_v2.Run(
            runId=run_id,
        )
        job = event_v2.Job(namespace=self.namespace, name=self.job_name, facets={})
        run_event = event_v2.RunEvent(
            eventType=event_v2.RunState.RUNNING,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=run,
            job=job,
            inputs=inputs,
            outputs=outputs,
        )
        self.client.emit(run_event)

    def post_graph_execute(
        self,
        run_id: str,
        graph: h_graph.FunctionGraph,
        success: bool,
        error: Optional[Exception],
        results: Optional[Dict[str, Any]],
    ):
        """

        :param run_id:
        :param graph:
        :param success:
        :param error:
        :param results:
        :return:
        """
        job = event_v2.Job(
            namespace=self.namespace,
            name=self.job_name,
        )
        facets = {}
        run_event_type = event_v2.RunState.COMPLETE
        if error:
            run_event_type = event_v2.RunState.FAIL
            error_message = str(error)
            facets = {
                "errorMessage": facet_v2.error_message_run.ErrorMessageRunFacet(
                    message=error_message,
                    stackTrace=get_stack_trace(error),
                    programmingLanguage="python",
                )
            }
        run = event_v2.Run(runId=run_id, facets=facets)

        run_event = event_v2.RunEvent(
            eventType=run_event_type,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=run,
            job=job,
        )
        self.client.emit(run_event)


# if __name__ == "__main__":
#     from openlineage.client import OpenLineageClient
#     from openlineage.client.transport.file import FileConfig, FileTransport
#
#     file_config = FileConfig(
#         log_file_path="/path/to/your/file",
#         append=False,
#     )
#
#     client = OpenLineageClient(transport=FileTransport(file_config))
#     namespace = "my_namespace"
#     db_datset = Dataset(namespace, name, facets)

import logging
from typing import Any, Dict, Optional

from hamilton import lifecycle

logger = logging.getLogger(__name__)
try:
    from ddtrace import Span, context, tracer
except ImportError as e:
    logger.error("ImportError: %s", e)
    logger.error(
        "To use the h_ddog plugin, please install sf-hamilton[datadog] using "
        "`pip install sf-hamilton[datadog]` (or use your favorite package manager)."
        "Remember to use quotes around the package name if using zsh!"
    )
    raise


class DDOGTracer(
    lifecycle.NodeExecutionHook, lifecycle.GraphExecutionHook, lifecycle.TaskExecutionHook
):
    """Lifecycle adapter to use datadog to run tracing on node execution. This works with the following execution environments:
    1. Vanilla Hamilton -- no task-based computation, just nodes
    2. Task-based, synchronous
    3. Task-based with Multithreading, Ray, and Dask
    It will likely work with others, although we have not yet tested them. This does not work with async (yet).

    Note that this is not a typical use of Datadog if you're not using hamilton for a microservice. It does work quite nicely, however!
    Monitoring ETLs is not a typical datadog case (you can't see relationships between nodes/tasks or data summaries),
    but it is easy enough to work with and gives some basic information.

    This tracer bypasses context management so we can more accurately track relationships between nodes/tags. Also, we plan to
    get this working with OpenTelemetry, and use that for datadog integration.

    To use this, you'll want to run `pip install sf-hamilton[ddog]` (or `pip install "sf-hamilton[ddog]"` if using zsh)
    """

    def __init__(self, root_name: str, include_causal_links: bool = False, service: str = None):
        """Creates a DDOGTracer. This has the option to specify some parameters.

        :param root_name: Name of the root trace/span. Due to the way datadog inherits, this will inherit an active span.
        :param include_causal_links: Whether or not to include span causal links. Note that there are some edge-cases here, and
            This is in beta for datadog, and actually broken in the current client, but it has been fixed and will be released shortly:
            https://github.com/DataDog/dd-trace-py/issues/8049. Furthermore, the query on datadog is slow for displaying causal links.
            We've disabled this by default, but feel free to test it out -- its likely they'll be improving the docum
        :param service: Service name -- will pick it up from the environment through DDOG if not available.
        """
        self.root_name = root_name
        self.service = service
        self.include_causal_links = include_causal_links
        self.run_span_cache = {}  # Cache of run_id -> span tuples
        self.task_span_cache = {}  # cache of run_iod -> task_id -> span. Note that we will prune this after task execution
        self.node_span_cache = {}  # Cache of run_id -> [task_id, node_id] -> span. We use this to open/close general traces

    @staticmethod
    def _serialize_span_dict(span_dict: Dict[str, Span]):
        """Serializes to a readable format. We're not propogating span links (see note above on causal links),
        but that's fine (for now). We have to do this as passing spans back and forth is frowned upon.

        :param span_dict: A key -> span dictionary
        :return: The serialized representation.
        """
        # For some reason this doesn't use the right ser/deser for dask
        # Or for some reason it has contexts instead of spans. Well, we can serialize them both!
        return {
            key: {
                "trace_id": span.context.trace_id if isinstance(span, Span) else span.trace_id,
                "span_id": span.context.span_id if isinstance(span, Span) else span.span_id,
            }
            for key, span in span_dict.items()
        }

    @staticmethod
    def _deserialize_span_dict(serialized_repr: Dict[str, dict]) -> Dict[str, context.Context]:
        """Note that we deserialize as contexts, as passing spans is not supported
        (the child should never terminate the parent span).

        :param span_dict: Dict of str -> dict params for contexts
        :return: A dictionary of contexts
        """
        return {key: context.Context(**params) for key, params in serialized_repr.items()}

    def __getstate__(self):
        """Gets the state for serialization"""
        return dict(
            root_trace_name=self.root_name,
            service=self.service,
            include_causal_links=self.include_causal_links,
            run_span_cache=self._serialize_span_dict(self.run_span_cache),
            task_span_cache={
                key: self._serialize_span_dict(value) for key, value in self.task_span_cache.items()
            },
            # this is unnecessary, but leaving it here for now
            # to remove it, we need to add a default check in the one that adds to the nodes
            node_span_cache={
                key: self._serialize_span_dict(value) for key, value in self.task_span_cache.items()
            },  # Nothing here, we can just wipe it for a new task
        )

    def __setstate__(self, state):
        """Sets the state for serialization"""
        self.service = state["service"]
        self.root_name = state["root_trace_name"]
        self.include_causal_links = state["include_causal_links"]
        # TODO -- move this out/consider doing it to the others
        self.run_span_cache = self._deserialize_span_dict(state["run_span_cache"])
        # We only really need this if we log the stuff before submitting...
        # This shouldn't happen but it leaves flexibility for the future
        self.task_span_cache = {
            key: self._deserialize_span_dict(value)
            for key, value in state["task_span_cache"].items()
        }
        self.node_span_cache = {
            key: self._deserialize_span_dict(value)
            for key, value in state["node_span_cache"].items()
        }

    @staticmethod
    def _sanitize_tags(tags: Dict[str, Any]) -> Dict[str, str]:
        """Sanitizes tags to be strings, just in case.

        :param tags: Node tags.
        :return: The string -> string representation of tags
        """
        return {f"hamilton.{key}": str(value) for key, value in tags.items()}

    def run_before_graph_execution(self, *, run_id: str, **future_kwargs: Any):
        """Runs before graph execution -- sets the state so future ones can reference it.

        :param run_id: ID of the run
        :param future_kwargs: reserved for future keyword arguments/backwards compatibility.
        """
        span = tracer.start_span(name=self.root_name, activate=True, service=self.service)
        self.run_span_cache[run_id] = span  # we save this as a root span
        self.node_span_cache[run_id] = {}
        self.task_span_cache[run_id] = {}

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_kwargs: Dict[str, Any],
        node_tags: Dict[str, Any],
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        """Runs before a node's execution. Sets up/stores spans.

        :param node_name: Name of the node.
        :param node_kwargs: Keyword arguments of the node.
        :param node_tags: Tags of the node (they'll get stored as datadog tags)
        :param task_id: Task ID that spawned the node
        :param run_id: ID of the run.
        :param future_kwargs: reserved for future keyword arguments/backwards compatibility.
        """
        # We need to do this on launching tasks and we have not yet exposed it.
        # TODO -- do pre-task and post-task execution.
        parent_span = self.task_span_cache[run_id].get(task_id) or self.run_span_cache[run_id]
        new_span_name = f"{task_id}:" if task_id is not None else ""
        new_span_name += node_name
        new_span = tracer.start_span(
            name=new_span_name, child_of=parent_span, activate=True, service=self.service
        )
        if self.include_causal_links:
            prior_spans = {
                key: self.node_span_cache[run_id].get((task_id, key)) for key in node_kwargs
            }
            for input_node, span in prior_spans.items():
                if span is not None:
                    new_span.link_span(
                        context=span.context,
                        attributes={
                            "link.name": f"{input_node}_to_{node_name}",
                        },
                    )
        tags = node_tags.copy()
        tags["hamilton.node_name"] = node_name
        new_span.set_tags(DDOGTracer._sanitize_tags(tags=tags))
        self.node_span_cache[run_id][(task_id, node_name)] = new_span

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        error: Optional[Exception],
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        """Runs after a node's execution -- completes the span.

        :param node_name: Name of the node
        :param error: Error that the node raised, if any
        :param task_id: Task ID that spawned the node
        :param run_id: ID of the run.
        :param future_kwargs: reserved for future keyword arguments/backwards compatibility.
        """
        span = self.node_span_cache[run_id][(task_id, node_name)]
        exc_type = None
        exc_value = None
        tb = None
        if error is not None:
            exc_type = type(error)
            exc_value = error
            tb = error.__traceback__
        span.__exit__(exc_type, exc_value, tb)

    def run_after_graph_execution(
        self, *, error: Optional[Exception], run_id: str, **future_kwargs: Any
    ):
        """Runs after graph execution. Garbage collects + finishes the root span.

        :param error: Error the graph raised when running, if any
        :param run_id: ID of the run
        :param future_kwargs: reserved for future keyword arguments/backwards compatibility.
        """
        span = self.run_span_cache[run_id]
        exc_type = None
        exc_value = None
        tb = None
        if error is not None:
            exc_type = type(error)
            exc_value = error
            tb = error.__traceback__
        span.__exit__(exc_type, exc_value, tb)
        del self.run_span_cache[run_id]
        del self.node_span_cache[run_id]
        del self.task_span_cache[run_id]

    def run_before_task_execution(self, *, task_id: str, run_id: str, **future_kwargs):
        """Runs before task execution. Sets up the task span.

        :param task_id: ID of the task
        :param run_id: ID of the run,
        :param future_kwargs: reserved for future keyword arguments/backwards compatibility.
        """
        parent_span = self.run_span_cache[run_id]
        self.task_span_cache[run_id][task_id] = tracer.start_span(
            name=task_id,
            child_of=parent_span,  # span or context both work
            activate=True,
            service=self.service,
        )

    def run_after_task_execution(
        self,
        *,
        task_id: str,
        run_id: str,
        error: Exception,
        **future_kwargs,
    ):
        """Rusn after task execution. Finishes task-level spans.

        :param task_id: ID of the task, ID of the run.
        :param run_id: ID of the run
        :param error: Error the graph raised when running, if any
        :param future_kwargs: Future keyword arguments for backwards compatibility
        """
        span = self.task_span_cache[run_id][task_id]
        exc_type = None
        exc_value = None
        tb = None
        if error is not None:
            exc_type = type(error)
            exc_value = error
            tb = error.__traceback__
        span.__exit__(exc_type, exc_value, tb)

from typing import Any, Dict, Optional, Tuple

from ddtrace import tracer

from hamilton import lifecycle


class DDOGTracer(lifecycle.NodeExecutionHook, lifecycle.GraphExecutionHook):
    """Lifecycle adapter to use DDOG to run tracing on node execution.
    Note this is not (yet) multithreading friendly, as we could get traces confused between threads. That said, it will probably work.
    """

    def __init__(self, root_trace_name: str, service: str = None):
        self.root_trace_name = root_trace_name
        self.service = service
        self.span_cache = (
            {}
        )  # Cache of (run_id, task_id, node_id) tuples -- carries multiple levels

    @staticmethod
    def _span_key(
        run_id: str, task_id: Optional[str], node_id: Optional[str]
    ) -> Tuple[Optional[str], ...]:
        return run_id, task_id, node_id

    def _cleanup(self, span_key: Tuple[Optional[str], ...]):
        del self.span_cache[span_key]

    @staticmethod
    def _sanitize_tags(tags: Dict[str, str]) -> Dict[str, str]:
        return {key: str(value) for key, value in tags.items()}

    def run_before_graph_execution(self, *, run_id: str, **future_kwargs: Any):
        span = tracer.start_span(name=self.root_trace_name, activate=True, service=self.service)
        span_key = DDOGTracer._span_key(run_id=run_id, task_id=None, node_id=None)
        self.span_cache[span_key] = span

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        # We need to do this on launching tasks and we have not yet exposed it.
        # TODO -- do pre-task and post-task execution.
        parent_span_key = DDOGTracer._span_key(run_id=run_id, task_id=None, node_id=None)
        parent_span = self.span_cache[parent_span_key]  # we need this to launch
        new_span_key = DDOGTracer._span_key(run_id, task_id, node_name)
        new_span_name = f"{task_id}:" if task_id is not None else ""
        new_span_name += node_name
        new_span = tracer.start_span(
            name=new_span_name, child_of=parent_span, activate=True, service=self.service
        )
        new_span.set_tags(DDOGTracer._sanitize_tags(tags=node_tags))
        self.span_cache[new_span_key] = new_span

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        result: Any,
        error: Optional[Exception],
        success: bool,
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        span_key = DDOGTracer._span_key(run_id=run_id, task_id=task_id, node_id=node_name)
        span = self.span_cache[span_key]
        exc_type = None
        exc_value = None
        tb = None
        if error is not None:
            exc_type = type(error)
            exc_value = error
            tb = error.__traceback__
        span.__exit__(exc_type, exc_value, tb)
        self._cleanup(span_key)

    def run_after_graph_execution(
        self, *, success: bool, error: Optional[Exception], run_id: str, **future_kwargs: Any
    ):
        span_key = DDOGTracer._span_key(run_id=run_id, task_id=None, node_id=None)
        span = self.span_cache[span_key]
        exc_type = None
        exc_value = None
        tb = None
        if error is not None:
            exc_type = type(error)
            exc_value = error
            tb = error.__traceback__
        span.__exit__(exc_type, exc_value, tb)
        self._cleanup(span_key)

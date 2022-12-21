import asyncio
import inspect
import logging
import sys
import time
import types
import typing
from types import ModuleType
from typing import Any, Dict, Optional, Tuple

from hamilton import base, driver, node, telemetry

logger = logging.getLogger(__name__)


async def await_dict_of_tasks(task_dict: Dict[str, types.CoroutineType]) -> Dict[str, Any]:
    """Util to await a dictionary of tasks as asyncio.gather is kind of garbage"""
    keys = sorted(task_dict.keys())
    coroutines = [task_dict[key] for key in keys]
    coroutines_gathered = await asyncio.gather(*coroutines)
    return dict(zip(keys, coroutines_gathered))


async def process_value(val: Any) -> Any:
    """Helper function to process the value of a potential awaitable.
    This is very simple -- all it does is await the value if its not already resolved.

    :param val: Value to process.
    :return: The value (awaited if it is a coroutine, raw otherwise).
    """
    if not inspect.isawaitable(val):
        return val
    return await val


class AsyncGraphAdapter(base.SimplePythonDataFrameGraphAdapter):
    def __init__(self, result_builder: base.ResultMixin = None):
        """Creates an AsyncGraphAdapter class. Note this will *only* work with the AsyncDriver class.

        Some things to note:
        1. This executes everything at the end (recursively). E.G. the final DAG nodes are awaited
        2. This does *not* work with decorators when the async function is being decorated. That is
        because that function is called directly within the decorator, so we cannot await it.
        """
        super(AsyncGraphAdapter, self).__init__()
        self.result_builder = result_builder if result_builder else base.PandasDataFrameResult()

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Executes a node. Note this doesn't actually execute it -- rather, it returns a task.
        This does *not* use async def, as we want it to be awaited on later -- this await is done
        in processing parameters of downstream functions/final results. We can ensure that as
        we also run the driver that this corresponds to.

        Note that this assumes that everything is awaitable, even if it isn't.
        In that case, it just wraps it in one.

        :param node: Node to wrap
        :param kwargs: Keyword arguments (either coroutines or raw values) to call it with
        :return: A task
        """
        callabl = node.callable

        async def new_fn(fn=callabl, **fn_kwargs):
            task_dict = {key: process_value(value) for key, value in fn_kwargs.items()}
            fn_kwargs = await await_dict_of_tasks(task_dict)
            if inspect.iscoroutinefunction(fn):
                return await (fn(**fn_kwargs))
            return fn(**fn_kwargs)

        coroutine = new_fn(**kwargs)
        task = asyncio.create_task(coroutine)
        return task

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Currently this is a no-op -- it just delegates to the resultsbuilder.
        That said, we *could* make it async, but it feels wrong -- this will just be
        called after `raw_execute`.

        :param outputs: Outputs (awaited) from the graph.
        :return: The final results.
        """
        return self.result_builder.build_result(**outputs)


class AsyncDriver(driver.Driver):
    def __init__(self, config, *modules, result_builder: Optional[base.ResultMixin] = None):
        """Instantiates an asynchronous driver.

        :param config: Config to build the graph
        :param modules: Modules to crawl for fns/graph nodes
        :param result_builder: Results mixin to compile the graph's final results. TBD whether this should be included in the long run.
        """
        super(AsyncDriver, self).__init__(
            config, *modules, adapter=AsyncGraphAdapter(result_builder=result_builder)
        )

    async def raw_execute(
        self,
        final_vars: typing.List[str],
        overrides: Dict[str, Any] = None,
        display_graph: bool = False,  # don't care
        inputs: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Executes the graph, returning a dictionary of strings (node keys) to final results.

        :param final_vars: Variables to execute (+ upstream)
        :param overrides: Overrides for nodes
        :param display_graph: whether or not to display graph -- this is not supported.
        :param inputs:  Inputs for DAG runtime calculation
        :return: A dict of key -> result
        """
        nodes, user_nodes = self.graph.get_upstream_nodes(final_vars, inputs)
        memoized_computation = dict()  # memoized storage
        self.graph.execute(nodes, memoized_computation, overrides, inputs)
        if display_graph:
            raise ValueError(
                "display_graph=True is not supported for the async graph adapter. "
                "Instead you should be using visualize_execution."
            )
        task_dict = {
            key: asyncio.create_task(process_value(memoized_computation[key])) for key in final_vars
        }
        return await await_dict_of_tasks(task_dict)

    async def execute(
        self,
        final_vars: typing.List[str],
        overrides: Dict[str, Any] = None,
        display_graph: bool = False,
        inputs: Dict[str, Any] = None,
    ) -> Any:
        """Executes computation.

        :param final_vars: the final list of variables we want to compute.
        :param overrides: values that will override "nodes" in the DAG.
        :param display_graph: DEPRECATED. Whether we want to display the graph being computed.
        :param inputs: Runtime inputs to the DAG.
        :return: an object consisting of the variables requested, matching the type returned by the GraphAdapter.
            See constructor for how the GraphAdapter is initialized. The default one right now returns a pandas
            dataframe.
        """
        if display_graph:
            raise ValueError(
                "display_graph=True is not supported for the async graph adapter. "
                "Instead you should be using visualize_execution."
            )
        start_time = time.time()
        run_successful = True
        error = None
        try:
            outputs = await self.raw_execute(final_vars, overrides, display_graph, inputs=inputs)
            result = self.adapter.build_result(**outputs)
            return result
        except Exception as e:
            run_successful = False
            logger.error(driver.SLACK_ERROR_MESSAGE)
            error = telemetry.sanitize_error(*sys.exc_info())
            raise e
        finally:
            duration = time.time() - start_time
            # ensure we can capture telemetry in async friendly way.
            if telemetry.is_telemetry_enabled():

                async def make_coroutine():
                    self.capture_execute_telemetry(
                        error, final_vars, inputs, overrides, run_successful, duration
                    )

                try:
                    # we don't have to await because we are running within the event loop.
                    asyncio.create_task(make_coroutine())
                except Exception as e:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.error(f"Encountered error submitting async telemetry:\n{e}")

    def capture_constructor_telemetry(
        self,
        error: Optional[str],
        modules: Tuple[ModuleType],
        config: Dict[str, Any],
        adapter: base.HamiltonGraphAdapter,
    ):
        """Ensures we capture constructor telemetry the right way in an async context.

        This is a simpler wrapper around what's in the driver class.

        :param error: sanitized error string, if any.
        :param modules: tuple of modules to build DAG from.
        :param config: config to create the driver.
        :param adapter: adapter class object.
        """
        if telemetry.is_telemetry_enabled():
            try:
                # check whether the event loop has been started yet or not
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.run_in_executor(
                        None,
                        super(AsyncDriver, self).capture_constructor_telemetry,
                        error,
                        modules,
                        config,
                        adapter,
                    )
                else:

                    async def make_coroutine():
                        super(AsyncDriver, self).capture_constructor_telemetry(
                            error, modules, config, adapter
                        )

                    loop.run_until_complete(make_coroutine())
            except Exception as e:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.error(f"Encountered error submitting async telemetry:\n{e}")

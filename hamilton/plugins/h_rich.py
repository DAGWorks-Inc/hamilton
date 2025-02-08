from typing import Any, List

from hamilton.execution.grouping import NodeGroupPurpose

try:
    from typing import override
except ImportError:
    override = lambda x: x  # noqa E731

import rich
from rich.console import Console
from rich.progress import Progress

from hamilton.lifecycle import GraphExecutionHook, TaskExecutionHook, TaskGroupingHook


class RichProgressBar(TaskExecutionHook, TaskGroupingHook, GraphExecutionHook):
    """An adapter that uses rich to show simple progress bars for the graph execution.

    Note: you need to have rich installed for this to work. If you don't have it installed, you can
    install it with `pip install rich` (or `pip install sf-hamilton[rich]` -- use quotes if you're
    using zsh).

    .. code-block:: python

        from hamilton.plugins import h_rich

        dr = (
            driver.Builder()
            .with_config({})
            .with_modules(some_modules)
            .with_adapters(h_rich.RichProgressBar())
            .build()
        )
        # and then when you call .execute() or .materialize() you'll get a progress bar!
    """

    def __init__(
        self, console: Console | None = None, group_desc: str = "", expand_desc: str = ""
    ) -> None:
        self._console = rich.get_console() if console is None else console
        self._group_desc = group_desc if group_desc else "Running Task Groups:"
        self._expand_desc = expand_desc if expand_desc else "Running Expanded Tasks:"
        self._progress = Progress(console=self._console)

    @override
    def run_before_graph_execution(self, **kwargs: Any):
        pass

    @override
    def run_after_graph_execution(self, **kwargs: Any):
        self._progress.stop()  # in case progress thread is lagging

    @override
    def run_after_task_grouping(self, *, task_ids: List[str], **kwargs):
        self._progress.add_task(self._group_desc, total=len(task_ids))
        self._progress.start()

    @override
    def run_after_task_expansion(self, *, parameters: dict[str, Any], **kwargs):
        self._progress.add_task(self._expand_desc, total=len(parameters))

    @override
    def run_before_task_execution(self, *, purpose: NodeGroupPurpose, **kwargs):
        if purpose == NodeGroupPurpose.GATHER:
            self._progress.advance(self._progress.task_ids[0])
            self._progress.stop_task(self._progress.task_ids[-1])

    @override
    def run_after_task_execution(self, *, purpose: NodeGroupPurpose, **kwargs):
        if purpose == NodeGroupPurpose.EXECUTE_BLOCK:
            self._progress.advance(self._progress.task_ids[-1])
        else:
            self._progress.advance(self._progress.task_ids[0])

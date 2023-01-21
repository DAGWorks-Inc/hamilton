import abc
import dataclasses
import enum
import json
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Dict, Optional, Type

import loguru

from hamilton import node as h_node
from hamilton.base import ResultMixin, SimplePythonGraphAdapter

logger = loguru.logger


class Status(enum.Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RUNNING = "RUNNING"
    UNINITIALIZED = "UNINITIALIZED"


def process_result(result: Any) -> Any:
    pass


@dataclass
class TaskRun:
    node_name: str
    status: Status = field(default_factory=lambda: Status.UNINITIALIZED)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    result_type: Optional[Type[Type]] = None
    result_summary: Optional[Any] = None  # TODO -- determine the best kind of result data here
    tracking_schema: int = 0  # Schema version for tracking
    # If this changes, everything should change
    # We should add a test that tests for any changes in this and ensures it gets bumpded


class TrackingState(abc.ABC):
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.status = None

    @abc.abstractmethod
    def update_task(self, task_name: str, task_run: TaskRun):
        pass

    @abc.abstractmethod
    def update_status(self, status: Status):
        pass

    @abc.abstractmethod
    def finalize(self, status: bool = Status.SUCCESS):
        pass


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        if isinstance(obj, enum.Enum):
            return obj.value
        if isinstance(obj, type):
            return str(obj)
        return super(CustomEncoder, self).default(obj)


class TrackingStateToJSONFile(TrackingState):
    def __init__(self, run_id: str, output_file: str):
        super().__init__(run_id)
        self.output_file = output_file
        self.task_map: Dict[str, TaskRun] = {}
        self.update_status(Status.UNINITIALIZED)

    def update_task(self, task_name: str, task_run: TaskRun):
        self.task_map.update({task_name: task_run})
        logger.info(task_run)

    def update_status(self, status: Status):
        self.status = status
        logger.info(status)

    def finalize(self):
        with open(self.output_file, "w") as f:
            json_object = {
                "tasks": list(sorted(self.task_map.values(), key=lambda x: x.start_time)),
                "status": self.status,
            }
            json.dump(json_object, f, cls=CustomEncoder)


class RunTrackingGraphAdapter(SimplePythonGraphAdapter):
    """This class allows you to track results of runs"""

    def __init__(self, result_builder: ResultMixin, tracking_state: TrackingState):
        """Tracks runs given run IDs. Note that this needs to be re-initialized on each run, we'll want to fix that.

        :param result_builder: Result builder to use
        :param run_id: Run ID to save with
        """
        super(RunTrackingGraphAdapter, self).__init__(result_builder=result_builder)
        self.result_builder = result_builder
        if self.result_builder is None:
            raise ValueError("You must provide a ResultMixin object for `result_builder`.")
        self.tracking_state = tracking_state

    def execute_node(self, node: h_node.Node, kwargs: Dict[str, Any]) -> Any:
        """Given a node that represents a hamilton function, execute it.
        Note, in some adapters this might just return some type of "future".

        :param node: the Hamilton Node
        :param kwargs: the kwargs required to exercise the node function.
        :return: the result of exercising the node.
        """
        # If the tracking state hasn't started
        if self.tracking_state.status == Status.UNINITIALIZED:
            self.tracking_state.update_status(Status.RUNNING)

        task_run = TaskRun(node_name=node.name)
        task_run.status = Status.RUNNING
        task_run.start_time = datetime.now()
        self.tracking_state.update_task(node.name, task_run)
        try:
            result = super().execute_node(node, kwargs)
            task_run.status = Status.SUCCESS
            task_run.result_type = type(result)
            task_run.result_data = process_result(result)
            task_run.end_time = datetime.now()
            self.tracking_state.update_task(node.name, task_run)
            return result
        except Exception as e:
            task_run.status = Status.FAILED
            task_run.end_time = datetime.now()
            self.tracking_state.update_status(Status.FAILED)
            self.tracking_state.update_task(node.name, task_run)
            self.tracking_state.finalize()  # Finalize within here so it gets done
            raise e

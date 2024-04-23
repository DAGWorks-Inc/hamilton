import dataclasses
import enum
from datetime import datetime
from typing import List, Optional, Type


class Status(enum.Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RUNNING = "RUNNING"
    UNINITIALIZED = "UNINITIALIZED"


@dataclasses.dataclass
class TaskRun:
    node_name: str
    status: Status = dataclasses.field(default_factory=lambda: Status.UNINITIALIZED)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    result_type: Optional[Type[Type]] = None
    # TODO -- determine the best kind of result data here
    result_summary: Optional[dict] = None
    error: Optional[List[str]] = None  # Serialization of error, broken into lines...
    is_in_sample: bool = True  # this isn't sent anywhere; so it's not in to_dict().

    def to_dict(self):
        return {
            "node_name": self.node_name,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "result_type": str(self.result_type),  # TODO -- fix this serialization up
            "result_summary": self.result_summary,  # TODO -- fix this serialization up
            "error": self.error,
        }


@dataclasses.dataclass
class DAGRun:
    """Represents a DAG run"""

    run_id: str
    status: Status
    tasks: List[TaskRun]
    start_time: datetime
    end_time: datetime
    schema_version: int

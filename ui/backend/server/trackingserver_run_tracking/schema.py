import datetime
from typing import List, Literal, Optional

from ninja import ModelSchema, Schema
from pydantic import BaseModel
from trackingserver_run_tracking.models import DAGRun, ExecutionStatus, NodeRun, NodeRunAttribute
from trackingserver_template.schema import CodeArtifactOut, NodeTemplateOut


class DAGRunIn(ModelSchema):
    class Meta:
        model = DAGRun
        exclude = ["id", "created_at", "updated_at", "dag_template"]


class DAGRunUpdate(Schema):
    # TODO -- find a better way of representing this
    run_status: Literal[tuple(ExecutionStatus.values)]
    run_end_time: datetime.datetime
    upsert_tags: Optional[dict] = None


class DAGRunOut(ModelSchema):
    class Meta:
        model = DAGRun
        fields = "__all__"

    username_resolved: Optional[str] = None
    dag_template_id: int

    @classmethod
    def create_with_username(cls, orm_model: DAGRun) -> "DAGRunOut":
        return DAGRunOut(
            **{
                **DAGRunOut.from_orm(orm_model).dict(),
                **{
                    "username_resolved": (
                        orm_model.launched_by.email if orm_model.launched_by else None
                    ),
                    "dag_template_id": orm_model.dag_template_id,
                },
            }
        )


class NodeRunIn(ModelSchema):
    class Meta:
        model = NodeRun
        exclude = ["id", "created_at", "updated_at", "dag_run"]


class NodeRunOut(ModelSchema):
    class Meta:
        model = NodeRun
        fields = "__all__"


class NodeRunAttributeIn(ModelSchema):
    class Meta:
        model = NodeRunAttribute
        exclude = ["id", "created_at", "updated_at", "dag_run"]


class NodeRunAttributeOut(ModelSchema):
    class Meta:
        model = NodeRunAttribute
        fields = "__all__"


class NodeRunOutWithAttributes(NodeRunOut):
    attributes: List[NodeRunAttributeOut]
    dag_run_id: int

    @classmethod
    def from_data(cls, node_run: NodeRun, attributes: List[NodeRunAttributeOut]):
        return NodeRunOutWithAttributes(
            **{
                **NodeRunOut.from_orm(node_run).dict(),
                **{"attributes": attributes, "dag_run_id": node_run.dag_run_id},
            }
        )


class DAGRunOutWithData(DAGRunOut):
    node_runs: List[NodeRunOutWithAttributes]
    dag_template_id: int

    @classmethod
    def from_data(cls, dag_run: DAGRun, node_runs: List[NodeRunOutWithAttributes]):
        return DAGRunOutWithData(
            **{
                **DAGRunOut.from_orm(dag_run).dict(),
                # Not sure why this isn't showing up -- todo, clean this up across the board
                **{"node_runs": node_runs, "dag_template_id": dag_run.dag_template_id},
            }
        )


class DagRunsBulkRequest(BaseModel):
    attributes: List[NodeRunAttributeIn]
    task_updates: List[NodeRunIn]


class NodeRunOutWithExtraData(NodeRunOut, BaseModel):
    dag_template_id: int
    dag_run_id: int

    @classmethod
    def from_orm(cls, obj, dag_template_id):
        node_run_out = NodeRunOut.from_orm(obj)
        return NodeRunOutWithExtraData(
            **node_run_out.dict(),
            dag_template_id=dag_template_id,
            dag_run_id=obj.dag_run_id,
        )


# This is probably not the best
# We're doing a weird join
# We should probably just have the right ofreign key
class CatalogZoomResponse(BaseModel):
    node_runs: List[NodeRunOutWithExtraData]
    node_templates: List[NodeTemplateOut]
    code_artifacts: List[CodeArtifactOut]
